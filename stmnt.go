package fsql

import (
	"context"
	"database/sql"

	"github.com/spearson78/fault"
	"github.com/spearson78/fault/fctx"
	"github.com/spearson78/fault/floc"
)

type fsqlStmt struct {
	stmt *sql.Stmt
	sql  string
}

func (m *fsqlStmt) Close() error {
	return m.stmt.Close()
}

// QueryContext executes a query, typically a SELECT that returns entities using the mapper function provided when preparing the statement. The args are for any placeholder parameters in the query. If zero rows are selected the returned slice will be nil.
func (m *fsqlStmt) QueryContext(ctx context.Context, args ...any) (*sql.Rows, error) {
	rows, err := m.stmt.QueryContext(ctx, args...)
	if err != nil {
		err = fault.Wrap(err, With(m.sql, args...), fctx.With(ctx), floc.WithDepth(1))
	}
	return rows, err
}

// Query executes a query, typically a SELECT that returns entities using the mapper function provided when preparing the statement.
func (m *fsqlStmt) Query(args ...any) (*sql.Rows, error) {
	rows, err := m.stmt.Query(args...)
	if err != nil {
		err = fault.Wrap(err, With(m.sql, args...), floc.WithDepth(1))
	}
	return rows, err

}

// QueryRowContext executes a query that is expected to return at most one row. The result row is mapped to an entity using the mapper function provided when preparing the statement. The args are for any placeholder parameters in the query. If the query selects no rows sql.ErrRows is returned. If multipe rows are returnd the frst row is mapped and returned.
func (m *fsqlStmt) QueryRowContext(ctx context.Context, args ...any) (*sql.Row, error) {
	row := m.stmt.QueryRowContext(ctx, args...)
	err := row.Err()
	if err != nil {
		row.Scan()
		err = fault.Wrap(err, With(m.sql, args...), fctx.With(ctx), floc.WithDepth(1))
	}
	return row, err
}

// QueryRow executes a query that is expected to return at most one row. The result row is mapped to an entity using the mapper function provided when preparing the statement. The args are for any placeholder parameters in the query. If the query selects no rows sql.ErrRows is returned. If multipe rows are returnd the frst row is mapped and returned.
func (m *fsqlStmt) QueryRow(args ...any) (*sql.Row, error) {
	row := m.stmt.QueryRow(args...)
	err := row.Err()
	if err != nil {
		row.Scan()
		err = fault.Wrap(err, With(m.sql, args...), floc.WithDepth(1))
	}
	return row, err
}

type prepare interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Prepare(query string) (*sql.Stmt, error)
}

// PrepareContext creates a prepared statement for later queries whose results will be mapped using the provided MapperFunc. Multiple queries or executions may be run concurrently from the returned statement. The caller must call the statement's Close method when the statement is no longer needed.
//
// The provided context is used for the preparation of the statement, not for the execution of the statement.
func PrepareContext(ctx context.Context, db prepare, query string) (*fsqlStmt, error) {
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fault.Wrap(err, With(query), fctx.With(ctx), floc.WithDepth(1))
	}

	return &fsqlStmt{
		stmt: stmt,
		sql:  query,
	}, nil
}

// PrepareContext creates a prepared statement for later queries whose results will be mapped using the provided MapperFunc. Multiple queries or executions may be run concurrently from the returned statement. The caller must call the statement's Close method when the statement is no longer needed.
func Prepare[E any](db prepare, query string) (*fsqlStmt, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, fault.Wrap(err, With(query), floc.WithDepth(1))
	}

	return &fsqlStmt{
		stmt: stmt,
		sql:  query,
	}, nil
}
