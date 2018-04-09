package gdo

import (
	"context"
	"database/sql"
)

type Map []map[string]interface{}
type GDO struct {
	*sql.DB
}

func New(db *sql.DB) GDO {
	return GDO{db}
}

func (g GDO) Exec(s *Statement) (ExecResult, error) {
	return g.ExecContext(context.Background(), s)
}

func (g GDO) ExecContext(ctx context.Context, s *Statement) (ExecResult, error) {
	var result sql.Result
	var err error

	if len(s.namedArgs) > 0 {
		s = processStatment(s)
	}

	result, err = g.DB.ExecContext(ctx, s.query, s.args...)

	if err != nil {
		return ExecResult{}, err
	}

	return ExecResult{executedStmt: s, Result: result}, nil
}

func (g GDO) Query(s *Statement) (QueryResult, error) {
	return g.QueryContext(context.Background(), s)
}

func (g GDO) QueryContext(ctx context.Context, s *Statement) (QueryResult, error) {
	var rows *sql.Rows
	var err error

	if len(s.namedArgs) > 0 {
		s = processStatment(s)
	}

	rows, err = g.DB.QueryContext(ctx, s.query, s.args...)

	if err != nil {
		return QueryResult{}, err
	}

	cols, err := rows.Columns()

	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{executedStmt: s, Rows: rows, Cols: cols}, nil
}

func insertAt(str, toIns string, pos int) string {
	return str[:pos] + toIns + str[pos+1:]
}
