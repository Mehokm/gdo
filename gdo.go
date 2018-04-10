package gdo

import (
	"context"
	"database/sql"
)

type Rows []Row
type Row map[string]interface{}

type queryCtxFn func(context.Context, string, ...interface{}) (*sql.Rows, error)
type execCtxFn func(context.Context, string, ...interface{}) (sql.Result, error)

type GDO struct {
	*sql.DB
}

func New(db *sql.DB) GDO {
	return GDO{db}
}

func (g GDO) Begin() (Transaction, error) {
	return g.BeginTx(context.Background(), nil)
}

func (g GDO) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	tx, err := g.DB.BeginTx(ctx, opts)

	return Transaction{tx}, err
}

func (g GDO) Exec(s *Statement) (ExecResult, error) {
	return g.ExecContext(context.Background(), s)
}

func (g GDO) ExecContext(ctx context.Context, s *Statement) (ExecResult, error) {
	return doExecCtx(g.DB.ExecContext, ctx, s)
}

func (g GDO) Query(s *Statement) (QueryResult, error) {
	return g.QueryContext(context.Background(), s)
}

func (g GDO) QueryContext(ctx context.Context, s *Statement) (QueryResult, error) {
	return doQueryCtx(g.DB.QueryContext, ctx, s)
}

func (g GDO) QueryRow(s *Statement) QueryRowResult {
	return g.QueryRowContext(context.Background(), s)
}

func (g GDO) QueryRowContext(ctx context.Context, s *Statement) QueryRowResult {
	return doQueryRowCtx(g.DB.QueryContext, ctx, s)
}

func insertAt(str, toIns string, pos int) string {
	return str[:pos] + toIns + str[pos+1:]
}

func doQueryCtx(fn queryCtxFn, ctx context.Context, s *Statement) (QueryResult, error) {
	var rows *sql.Rows
	var err error

	if len(s.namedArgs) > 0 {
		s, err = processStatment(s)

		if err != nil {
			return QueryResult{}, err
		}
	}

	rows, err = fn(ctx, s.query, s.args...)

	if err != nil {
		return QueryResult{}, err
	}

	cols, err := rows.Columns()

	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{GDOResult: GDOResult{executedStmt: s}, Rows: rows, Cols: cols}, nil
}

func doQueryRowCtx(fn queryCtxFn, ctx context.Context, s *Statement) QueryRowResult {
	rs, err := doQueryCtx(fn, ctx, s)

	if err != nil {
		return QueryRowResult{err: err}
	}

	return QueryRowResult{QueryResult: rs, err: nil}
}

func doExecCtx(fn execCtxFn, ctx context.Context, s *Statement) (ExecResult, error) {
	var result sql.Result
	var err error

	if len(s.namedArgs) > 0 {
		s, err = processStatment(s)

		if err != nil {
			return ExecResult{}, err
		}
	}

	result, err = fn(ctx, s.query, s.args...)

	if err != nil {
		return ExecResult{}, err
	}

	return ExecResult{GDOResult: GDOResult{executedStmt: s}, Result: result}, nil
}
