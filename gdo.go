package gdo

import (
	"context"
	"database/sql"
	"index/suffixarray"
	"sort"
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
		namedArgMap := make(map[string][]int)

		var toReplace []string

		index := suffixarray.New([]byte(query))
		inds := index.Lookup([]byte(":"), -1)

		sort.Ints(inds)

		// TODO: should check if an even amount
		// throw error about malformed parameters

		l := len(query)
		k := 0
		for i := 0; i < len(inds)-1; i += 2 {
			j, jj := inds[i], inds[i+1]

			var argName string
			if jj < l {
				argName = query[j+1 : jj]
			} else {
				argName = query[j+1:]
			}

			namedArgMap[argName] = append(namedArgMap[argName], k)

			toReplace = append(toReplace, delim+argName+delim, "?")

			k++
		}

		replacedSQL = strings.NewReplacer(toReplace...).Replace(query)

		qna = queryNamedArgs{
			dict:  namedArgMap,
			total: len(inds) / 2, // assume even because double delim
		}
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
	indexFirst := strings.Index(query, ":")

	if indexFirst < 0 {
		return false
	}

	for i := indexFirst + 1; i < len(query); i++ {
		if unicode.IsSpace(rune(query[i])) {
			return false
		} else if query[i] == ':' {
			return true
		}
	}

	return false
}
