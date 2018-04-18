package gdo

import (
	"context"
	"database/sql"
	"index/suffixarray"
	"sort"
	"strings"
)

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

func (g GDO) Prepare(query string) (*PreparedStatement, error) {
	return g.prepareContext(context.Background(), query)
}

func (g GDO) PrepareContext(ctx context.Context, query string) (*PreparedStatement, error) {
	return g.prepareContext(ctx, query)
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

func (g GDO) prepareContext(ctx context.Context, query string) (*PreparedStatement, error) {
	namedArgMap := make(map[string][]int)

	var toReplace []string

	index := suffixarray.New([]byte(query))
	inds := index.Lookup([]byte("@"), -1)

	sort.Ints(inds)

	for i, ind := range inds {
		var argName string

		ii := strings.Index(query[ind:], " ")

		if ii < 0 {
			argName = query[ind:]
		} else {
			argName = query[ind:][:ii]
		}

		namedArgMap[argName] = append(namedArgMap[argName], i)

		toReplace = append(toReplace, argName, "?")
	}

	replacedSQL := strings.NewReplacer(toReplace...).Replace(query)

	ps, err := g.DB.PrepareContext(ctx, replacedSQL)

	if err != nil {
		return &PreparedStatement{}, err
	}

	return &PreparedStatement{
		Stmt: ps,
		Statement: &Statement{
			query:     replacedSQL,
			namedArgs: make([]sql.NamedArg, 0),
			args:      make([]interface{}, 0),
		},
		queryNamedArgs: queryNamedArgs{
			dict:  namedArgMap,
			total: len(inds),
		},
	}, nil
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

	return QueryResult{
		GDOResult: GDOResult{
			executedStmt: s,
		},
		Rows: rows, Cols: cols,
	}, nil
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

	return ExecResult{
		GDOResult: GDOResult{
			executedStmt: s,
		},
		Result: result,
	}, nil
}
