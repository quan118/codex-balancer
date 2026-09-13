package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel/trace"
)

// Each operation owns its result and lifetime; callers own only their wait.
// An abandoned exchange must finish before a later caller starts another one.
type accountRefresh struct {
	done      chan struct{}
	cancel    context.CancelFunc
	waiters   int
	abandoned bool
	err       error // published by closing done
	observed  *refreshObservation
}

type refreshOwner struct {
	lifetime   context.Context
	client     *http.Client
	operations *refreshOperations
	tracer     trace.Tracer
	log        *slog.Logger
	persist    func(context.Context, accountState) (accountState, error)
	completed  func(context.Context, error) error
}

func (a *Account) refresh(ctx context.Context, hc *http.Client, persist func(accountState) (accountState, error)) error {
	owner := refreshOwner{lifetime: context.Background(), client: hc}
	if persist != nil {
		owner.persist = func(_ context.Context, state accountState) (accountState, error) { return persist(state) }
	}
	return a.refreshOwned(ctx, owner)
}

func (a *Account) refreshOwned(wait context.Context, owner refreshOwner) error {
	for {
		if err := wait.Err(); err != nil {
			return err
		}
		a.mu.Lock()
		operation := a.inflight
		if operation != nil && operation.abandoned {
			a.mu.Unlock()
			select {
			case <-wait.Done():
				return wait.Err()
			case <-operation.done:
				continue
			}
		}
		first := operation == nil
		if operation == nil {
			if err := owner.lifetime.Err(); err != nil {
				a.mu.Unlock()
				return err
			}
			if a.Reauth != "" {
				err := fmt.Errorf("account %s needs reauth: %s", claimsFromToken(a.IDToken).Auth.AccountID, a.Reauth)
				a.mu.Unlock()
				return err
			}
			completionParent := context.Background()
			if owner.operations != nil {
				var registered bool
				completionParent, registered = owner.operations.start()
				if !registered {
					a.mu.Unlock()
					return context.Canceled
				}
			}
			ctx, cancel := context.WithTimeout(owner.lifetime, refreshTimeout)
			ctx, observed := observeRefresh(ctx, wait, owner.tracer, owner.log, claimsFromToken(a.IDToken).Auth.AccountID)
			operation = &accountRefresh{done: make(chan struct{}), cancel: cancel, observed: observed}
			a.inflight = operation
			state := a.accountState
			go func() {
				var completionErr error
				if owner.operations != nil {
					defer func() { owner.operations.finish(completionErr) }()
				}
				defer observed.span.End() // before registration is released/exporter shutdown
				observed.outcome(ctx, "exchange_started", nil)
				tokens, permanent, err := exchangeRefreshToken(ctx, owner.client, state.RefreshToken)
				observed.outcome(ctx, "exchange_finished", err)
				cancel()
				// Decoded results must survive waiter/runtime cancellation. Only
				// the completion budget (or its shutdown grace) can interrupt this
				// phase, including persistence and unavailable-owner notification.
				completion, stop := context.WithTimeout(completionParent, refreshCompletionTimeout)
				completion = trace.ContextWithSpan(completion, observed.span)
				completion, completionSpan := observed.tracer.Start(completion, "codex.credentials.complete")
				err, completionErr = a.finishRefresh(completion, owner.persist, state, tokens, permanent, err)
				if owner.completed != nil {
					notifyErr := owner.completed(completion, err)
					err = errors.Join(err, notifyErr)
					completionErr = errors.Join(completionErr, notifyErr)
				}
				observed.outcome(completion, "publication_finished", completionErr)
				spanFailure(completionSpan, completionErr)
				completionSpan.End()
				spanFailure(observed.span, err)
				stop()
				a.mu.Lock()
				operation.err = err
				a.inflight = nil
				close(operation.done)
				a.mu.Unlock()
			}()
		}
		operation.waiters++
		waiterCount := operation.waiters
		a.mu.Unlock()
		operation.observed.waiter(wait, first, waiterCount)

		var err error
		select {
		case <-wait.Done():
			err = wait.Err()
		case <-operation.done:
			err = operation.err
		}
		a.mu.Lock()
		operation.waiters--
		if operation.waiters == 0 && a.inflight == operation {
			operation.abandoned = true
			operation.cancel()
		}
		a.mu.Unlock()
		return err
	}
}
