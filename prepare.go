package gdo

import (
	"context"
	"database/sql"
)

type queryCtxPreparedFunc func(context.Context, ...interface{}) (*sql.Rows, error)
type execCtxPreparedFunc func(context.Context, ...interface{}) (sql.Result, error)

type queryNamedArgs struct {
	dict  map[string][]int
	total int
}

type PreparedStatement struct {
	*Statement
	*sql.Stmt
	queryNamedArgs queryNamedArgs
}

func (ps *PreparedStatement) Exec() (ExecResult, error) {
	return ps.ExecContext(context.Background())
}

func (ps *PreparedStatement) ExecContext(ctx context.Context) (ExecResult, error) {
	return doPreparedExecCtx(ctx, ps.Stmt.ExecContext, ps)
}

func (ps *PreparedStatement) Query() (QueryResult, error) {
	return ps.QueryContext(context.Background())
}

func (ps *PreparedStatement) QueryContext(ctx context.Context) (QueryResult, error) {
	return doPreparedQueryCtx(ctx, ps.Stmt.QueryContext, ps)
}

func (ps *PreparedStatement) QueryRow() QueryRowResult {
	return ps.QueryRowContext(context.Background())
}

func (ps *PreparedStatement) QueryRowContext(ctx context.Context) QueryRowResult {
	return doPreparedQueryRowCtx(ctx, ps.Stmt.QueryContext, ps)
}

func doPreparedQueryCtx(ctx context.Context, fn queryCtxPreparedFunc, ps *PreparedStatement) (QueryResult, error) {
	var rows *sql.Rows
	var err error

	if ps.isParamertized && len(ps.namedArgs) > 0 {
		ps, err = processPreparedStatement(ps) // get args for query

		if err != nil {
			return QueryResult{}, err
		}
	}

	rows, err = fn(ctx, ps.args...)

	if err != nil {
		return QueryResult{}, err
	}

	cols, err := rows.Columns()

	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		GDOResult: GDOResult{
			executedStmt: ps.Statement,
		},
		Rows: rows, Cols: cols,
	}, nil
}

func doPreparedQueryRowCtx(ctx context.Context, fn queryCtxPreparedFunc, ps *PreparedStatement) QueryRowResult {
	rs, err := doPreparedQueryCtx(ctx, fn, ps)

	if err != nil {
		return QueryRowResult{err: err}
	}

	return QueryRowResult{QueryResult: rs, err: nil}
}

func doPreparedExecCtx(ctx context.Context, fn execCtxPreparedFunc, ps *PreparedStatement) (ExecResult, error) {
	var result sql.Result
	var err error

	if ps.isParamertized && len(ps.namedArgs) > 0 {
		ps, err = processPreparedStatement(ps) // get args for query

		if err != nil {
			return ExecResult{}, err
		}
	}

	result, err = fn(ctx, ps.args...)

	if err != nil {
		return ExecResult{}, err
	}

	return ExecResult{
		GDOResult: GDOResult{
			executedStmt: ps.Statement,
		},
		Result: result,
	}, nil
}

func processPreparedStatement(ps *PreparedStatement) (*PreparedStatement, error) {
	args := make([]interface{}, ps.queryNamedArgs.total)

	for _, namedArg := range ps.namedArgs {
		inds := ps.queryNamedArgs.dict[namedArg.Name]

		for _, k := range inds {
			args[k] = namedArg.Value
		}
	}

	return &PreparedStatement{
		Stmt: ps.Stmt,
		Statement: &Statement{
			query:     ps.query,
			namedArgs: ps.namedArgs,
			args:      args,
		},
	}, nil
}
