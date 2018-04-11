package gdo

import (
	"database/sql"
)

type GDOResult struct {
	executedStmt *Statement
}

type ExecResult struct {
	GDOResult
	sql.Result
}

type QueryResult struct {
	GDOResult
	Rows     *sql.Rows
	Cols     []string
	colTypes []*sql.ColumnType
}

type QueryRowResult struct {
	QueryResult
	err error
}

func (r QueryResult) FetchRows() Rows {
	var m Rows

	results := make([]interface{}, len(r.Cols))
	rawResults := make([]interface{}, len(r.Cols))

	for i := range rawResults {
		rawResults[i] = &results[i]
	}

	defer r.Rows.Close()

	for r.Rows.Next() {
		assoc := make(map[string]interface{})

		r.Rows.Scan(rawResults...)

		for i := range results {
			assoc[r.Cols[i]] = results[i]
		}

		m = append(m, assoc)
	}

	return m
}

func (qrr QueryRowResult) FetchRow() Row {
	var r Row

	if qrr.Rows == nil {
		return r
	}

	rs := qrr.FetchRows()

	if len(rs) < 1 {
		return r
	}

	return rs[0]
}

func (qrr QueryRowResult) Error() error {
	return qrr.err
}

func (r GDOResult) LastExecutedQuery() string {
	return r.executedStmt.lastExecutedQuery()
}
