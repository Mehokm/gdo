package gdo

import (
	"context"
	"database/sql"
	"strings"
	"unicode"
)

const delim = ":"

type queryCtxFunc func(context.Context, string, ...interface{}) (*sql.Rows, error)
type execCtxFunc func(context.Context, string, ...interface{}) (sql.Result, error)

type GDO struct {
	*sql.DB
}

func New(db *sql.DB) *GDO {
	return &GDO{db}
}

func (g GDO) BeginTx() (Transaction, error) {
	return g.BeginTxContext(context.Background(), nil)
}

func (g GDO) BeginTxContext(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
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
	replacedSQL := query
	var qna queryNamedArgs

	isParameterized := checkIsParameterized(replacedSQL)

	if isParameterized {
		var toReplace []string

		qna = getQueryParameters(query)

		for key := range qna.dict {
			toReplace = append(toReplace, delim+key+delim, "?")
		}

		replacedSQL = strings.NewReplacer(toReplace...).Replace(query)
	}

	ps, err := g.DB.PrepareContext(ctx, replacedSQL)

	if err != nil {
		return &PreparedStatement{}, err
	}

	return &PreparedStatement{
		Stmt: ps,
		Statement: &Statement{
			query:           replacedSQL,
			namedArgs:       make([]sql.NamedArg, 0),
			args:            make([]interface{}, 0),
			isParameterized: isParameterized,
		},
		queryNamedArgs: qna,
	}, nil
}

func doQueryCtx(fn queryCtxFunc, ctx context.Context, s *Statement) (QueryResult, error) {
	var rows *sql.Rows
	var err error

	if s.isParameterized && len(s.namedArgs) > 0 {
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

func doQueryRowCtx(fn queryCtxFunc, ctx context.Context, s *Statement) QueryRowResult {
	rs, err := doQueryCtx(fn, ctx, s)

	if err != nil {
		return QueryRowResult{err: err}
	}

	return QueryRowResult{QueryResult: rs, err: nil}
}

func doExecCtx(fn execCtxFunc, ctx context.Context, s *Statement) (ExecResult, error) {
	var result sql.Result
	var err error

	if s.isParameterized && len(s.namedArgs) > 0 {
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

func checkIsParameterized(query string) bool {
	var paramCount []bool

	for i, r := range query {
		if r == ':' {
			for j := i + 1; j < len(query); j++ {
				if query[j] == ':' {
					paramCount = append(paramCount, true)
					break
				} else if unicode.IsSpace(rune(query[j])) {
					break
				}
			}
		}
	}

	return len(paramCount) > 0
}

func getQueryParameters(query string) queryNamedArgs {
	qnp := queryNamedArgs{
		dict: make(map[string][]int),
	}

	var order int
	for i, r := range query {
		if r == ':' {
			for j, start := i+1, i+1; j < len(query); j++ {
				if query[j] == ':' {
					qnp.dict[query[start:j]] = append(qnp.dict[query[start:j]], order)

					order++
					break
				} else if unicode.IsSpace(rune(query[j])) {
					break
				}
			}
		}
	}

	// keeping track of the order also keeps the total at the end
	qnp.total = order

	return qnp
}
