package state

const modelsSchema = `ALTER TABLE settings ADD COLUMN models_client_version TEXT NOT NULL DEFAULT '';`

func (s *Store) migrateModelsClientVersion() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var present int
	if err := tx.QueryRow("SELECT count(*) FROM pragma_table_info('settings') WHERE name = 'models_client_version'").Scan(&present); err != nil {
		return err
	}
	if present == 0 {
		if _, err := tx.Exec(modelsSchema); err != nil {
			return err
		}
	}
	if _, err := tx.Exec("PRAGMA user_version = 8"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ModelsClientVersion() (string, error) {
	var version string
	err := s.db.QueryRow("SELECT models_client_version FROM settings WHERE id = 1").Scan(&version)
	return version, err
}

func (s *Store) SetModelsClientVersion(version string) error {
	_, err := s.db.Exec("UPDATE settings SET models_client_version = ? WHERE id = 1", version)
	return err
}
