package app

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

const settingsHelp = `Manage global settings.

Usage:
  codex-balancer settings set fast-mode <default|on|off>
  codex-balancer settings set blocked-emails <email[,email...]|"">
  codex-balancer settings get fast-mode
  codex-balancer settings get blocked-emails
  codex-balancer settings list [-json]

Fast mode: default respects the client; on forces fast; off forces standard.
Blocked emails exclude matching accounts from routing (case-insensitive). Empty clears the list.
Fast mode changes restart existing WebSockets. Blocking an email closes that account's connections.

Flags (place before the setting name):
  -state string  state database (default %s)
  -json          machine-readable output, get/list only
`

func settingsCmd(args []string) error {
	help := func() { fmt.Fprintf(os.Stdout, settingsHelp, defaultStatePath()) }
	if len(args) == 0 {
		help()
		return errors.New("no subcommand given")
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		help()
		return nil
	}
	fs := flag.NewFlagSet("settings", flag.ContinueOnError)
	fs.Usage = help
	path := fs.String("state", defaultStatePath(), "state database")
	asJSON := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "set":
		if fs.NArg() != 2 || *asJSON {
			return errors.New("usage: settings set [-state path] <fast-mode|blocked-emails> <value>")
		}
		if fs.Arg(0) != "blocked-emails" && (fs.Arg(0) != "fast-mode" || !fastMode(fs.Arg(1)).valid()) {
			return errors.New("usage: settings set [-state path] <fast-mode|blocked-emails> <value>")
		}
	case "get":
		if fs.NArg() != 1 || (fs.Arg(0) != "fast-mode" && fs.Arg(0) != "blocked-emails") {
			return errors.New("usage: settings get [-json] [-state path] <fast-mode|blocked-emails>")
		}
	case "list":
		if fs.NArg() != 0 {
			return errors.New("usage: settings list [-json] [-state path]")
		}
	default:
		return fmt.Errorf("unknown settings subcommand %q", args[0])
	}
	store, err := openStateStore(*path)
	if err != nil {
		return err
	}
	defer store.Close()
	if args[0] == "set" {
		if fs.Arg(0) == "fast-mode" {
			if err := store.raw.SetFastMode(fs.Arg(1)); err != nil {
				return err
			}
			fmt.Printf("Saved fast-mode=%s. Running servers apply changes on their next settings poll (500 ms).\n", fs.Arg(1))
		} else {
			emails, err := parseBlockedEmails(fs.Arg(1))
			if err != nil {
				return err
			}
			encoded, err := json.Marshal(emails)
			if err != nil {
				return err
			}
			if err := store.raw.SetBlockedEmails(string(encoded)); err != nil {
				return err
			}
			fmt.Printf("Saved blocked-emails=%s. Running servers apply changes on their next settings poll (500 ms).\n", strings.Join(emails, ","))
		}
		return nil
	}
	mode, err := store.raw.FastMode()
	if err != nil {
		return err
	}
	blocked, err := store.raw.BlockedEmails()
	if err != nil {
		return err
	}
	var emails []string
	if err := json.Unmarshal([]byte(blocked), &emails); err != nil {
		return err
	}
	if *asJSON {
		values := map[string]any{"fast-mode": mode, "blocked-emails": emails}
		if args[0] == "get" {
			values = map[string]any{fs.Arg(0): values[fs.Arg(0)]}
		}
		return json.NewEncoder(os.Stdout).Encode(values)
	}
	if args[0] == "get" {
		if fs.Arg(0) == "fast-mode" {
			fmt.Println(mode)
		} else {
			fmt.Println(strings.Join(emails, ","))
		}
	} else {
		fmt.Printf("fast-mode\t%s\n", mode)
		fmt.Printf("blocked-emails\t%s\n", strings.Join(emails, ","))
	}
	return nil
}
