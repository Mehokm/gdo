package gdo

import (
	"database/sql"
)

type QueryResult struct {
	executedStmt *Statement
	Rows         *sql.Rows
	Cols         []string
}

func (r QueryResult) FetchMap() Map {
	var m Map

	result := make([]sql.RawBytes, len(r.Cols))
	rawResult := make([]interface{}, len(r.Cols))

	for i := range rawResult {
		rawResult[i] = &result[i]
	}

	defer r.Rows.Close()

	for r.Rows.Next() {
		assoc := make(map[string]interface{})

		r.Rows.Scan(rawResult...)

		for i := range result {
			assoc[r.Cols[i]] = string(result[i])
		}

		m = append(m, assoc)
	}

	return m
}

func (r QueryResult) LastExecutedQuery() string {
	return r.executedStmt.lastExecutedQuery()
}
