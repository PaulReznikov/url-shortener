package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq" //init postgres driver
	"url-shortener/internal/storage"
)

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	const op = "storage.postgres.New" //operation

	db, err := sql.Open("postgres", storagePath)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	//first query
	stmt1, err := db.Prepare(`
	CREATE TABLE IF NOT EXISTS url(
	    id SERIAL PRIMARY KEY,
	    alias TEXT UNIQUE NOT NULL,
	    url TEXT NOT NULL);
	`)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	defer stmt1.Close()

	_, err = stmt1.Exec()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	//second query
	stmt2, err := db.Prepare(`CREATE INDEX IF NOT EXISTS idx_alias ON url(alias);`)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	defer stmt2.Close()

	_, err = stmt2.Exec()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) SaveURL(urlToSave string, alias string) (int64, error) {
	const op = "storage.postgres.SaveURL"
	var id int64

	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(`INSERT INTO url(alias, url)
									VALUES($1, $2) RETURNING id`)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	defer stmt.Close()

	err = stmt.QueryRow(alias, urlToSave).Scan(&id)
	if err != nil {
		//TODO refactor this
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			// Код 23505 означает нарушение уникального ограничения
			return 0, fmt.Errorf("%s: %w", op, storage.ErrURLExists)
		}
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	err = tx.Commit()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return id, nil
}

func (s *Storage) GetURL(alias string) (string, error) {
	const op = "storage.postgres.GetURL"
	var url string
	tx, err := s.db.Begin()
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(`SELECT url FROM url
				WHERE alias = $1`)
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	defer stmt.Close()

	err = stmt.QueryRow(alias).Scan(&url)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Обработка ситуации, когда запрос не вернул данных
			return "", fmt.Errorf("%s: %w", op, storage.ErrURLNotFound)
		}
		return "", fmt.Errorf("%s: %w", op, err)
	}

	err = tx.Commit()
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	return url, nil
}

func (s *Storage) DeleteURL(alias string) error {
	const op = "storage.postgres.DeleteURL"

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(`DELETE FROM url
									WHERE alias = $1`)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	defer stmt.Close()

	result, err := stmt.Exec(alias)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if rowsAffected < 1 {
		return fmt.Errorf("%s: %w", op, sql.ErrNoRows)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil

}
