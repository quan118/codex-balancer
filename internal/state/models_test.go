package state

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func TestMigrateModelsClientVersionAddsColumnOnce(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprintf("column_present_%t", present), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			if _, err := db.Exec(fmt.Sprintf("PRAGMA application_id = %d; PRAGMA user_version = 7;", ApplicationID) + currentSchema); err != nil {
				t.Fatal(err)
			}
			if !present {
				if _, err := db.Exec("ALTER TABLE settings DROP COLUMN models_client_version"); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			var version int
			if err := store.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != schemaVersion {
				t.Fatalf("version = %d, error = %v", version, err)
			}
			if got, err := store.ModelsClientVersion(); err != nil || got != "" {
				t.Fatalf("initial version = %q, error = %v", got, err)
			}
			if err := store.SetModelsClientVersion("0.154.0"); err != nil {
				t.Fatal(err)
			}
			if got, err := store.ModelsClientVersion(); err != nil || got != "0.154.0" {
				t.Fatalf("version = %q, error = %v", got, err)
			}
		})
	}
}
