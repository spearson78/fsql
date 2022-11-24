package fsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Southclaws/fault"
	"github.com/Southclaws/fault/fctx"
	"github.com/Southclaws/fault/floc"
)

type exec interface {
	Exec(string, ...any) (sql.Result, error)
}

type query interface {
	Query(string, ...any) (*sql.Rows, error)
}

type queryContext interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type queryRow interface {
	QueryRow(string, ...any) *sql.Row
}

type queryRowContext interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
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

func Exec(db exec, sql string, params ...any) (sql.Result, error) {
	r, err := db.Exec(sql, params...)
	if err != nil {
		err = fault.Wrap(err, With(sql, params...), floc.WithDepth(1))
	}
	return r, err
}

func Query(db query, sql string, params ...any) (*sql.Rows, error) {
	r, err := db.Query(sql, params...)
	return r, fault.Wrap(err, With(sql, params...), floc.WithDepth(1))
}

func QueryContext(ctx context.Context, db queryContext, sql string, params ...any) (*sql.Rows, error) {
	r, err := db.QueryContext(ctx, sql, params...)
	if err != nil {
		err = fault.Wrap(err, fctx.With(ctx), With(sql, params...), floc.WithDepth(1))
	}
	return r, err
}

func QueryRow(db queryRow, sql string, params ...any) (*sql.Row, error) {
	r := db.QueryRow(sql, params...)
	err := r.Err()
	if err != nil {
		r.Scan()
		err = fault.Wrap(err, With(sql, params...), floc.WithDepth(1))
	}
	return r, err
}

func QueryRowContext(ctx context.Context, db queryRowContext, sql string, params ...any) (*sql.Row, error) {
	r := db.QueryRowContext(ctx, sql, params...)
	err := r.Err()
	if err != nil {
		r.Scan()
		err = fault.Wrap(err, fctx.With(ctx), With(sql, params...), floc.WithDepth(1))
	}
	return r, err
}
