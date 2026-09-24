package state

import "fmt"

const blockedEmailsSchema = `ALTER TABLE settings ADD COLUMN blocked_emails TEXT NOT NULL DEFAULT '[]';`

func (s *Store) migrateBlockedEmails() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var present int
	if err := tx.QueryRow("SELECT count(*) FROM pragma_table_info('settings') WHERE name = 'blocked_emails'").Scan(&present); err != nil {
		return err
	}
	if present == 0 {
		if _, err := tx.Exec(blockedEmailsSchema); err != nil {
			return fmt.Errorf("migrate blocked emails: %w", err)
		}
	}
	if _, err := tx.Exec("PRAGMA user_version = 9"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) BlockedEmails() (string, error) {
	var value string
	err := s.db.QueryRow("SELECT blocked_emails FROM settings WHERE id = 1").Scan(&value)
	return value, err
}

func (s *Store) SetBlockedEmails(value string) error {
	_, err := s.db.Exec("UPDATE settings SET blocked_emails = ? WHERE id = 1", value)
	return err
}
