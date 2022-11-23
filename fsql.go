package fsql

import (
	"database/sql"
	"errors"
	"fmt"
)

type db interface {
	Exec(string, ...any) (sql.Result, error)
	Query(string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
}

type withSql struct {
	wrapped error
	sql     string
	params  []any
}

func (e *withSql) Error() string  { return fmt.Sprintf("<sql> : %v", e.wrapped) }
func (e *withSql) Cause() error   { return e.wrapped }
func (e *withSql) Unwrap() error  { return e.wrapped }
func (e *withSql) String() string { return e.Error() }

func Wrap(err error, sql string, params ...any) error {
	if err == nil {
		return nil
	}

	return &withSql{
		wrapped: err,
		sql:     sql,
		params:  params,
	}
}

func With(sql string, params ...any) func(error) error {
	return func(err error) error {
		return Wrap(err, sql, params...)
	}
}

func Get(err error) (string, interface{}, bool) {
	if err == nil {
		return "", nil, false
	}

	var with *withSql
	if errors.As(err, &with) {
		return with.sql, with.params, true
	}

	return "", nil, false
}

func Exec(db db, sql string, params ...any) (sql.Result, error) {
	r, err := db.Exec(sql, params...)
	return r, Wrap(err, sql, params...)
}

func Query(db db, sql string, params ...any) (*sql.Rows, error) {
	r, err := db.Query(sql, params...)
	return r, Wrap(err, sql, params...)
}

func QueryRow(db db, sql string, params ...any) (*sql.Row, error) {
	r := db.QueryRow(sql, params...)
	return r, Wrap(r.Err(), sql, params...)
}
