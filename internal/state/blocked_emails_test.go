package state

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func TestMigrateBlockedEmailsPreservesSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA application_id = %d; PRAGMA user_version = 8;", ApplicationID) + currentSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("ALTER TABLE settings DROP COLUMN blocked_emails; UPDATE settings SET fast_mode = 'on'"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got, err := store.FastMode(); err != nil || got != "on" {
		t.Fatalf("fast mode = %q, %v", got, err)
	}
	if got, err := store.BlockedEmails(); err != nil || got != "[]" {
		t.Fatalf("blocked emails = %q, %v", got, err)
	}
}
