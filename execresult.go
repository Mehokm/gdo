package gdo

import "database/sql"

type ExecResult struct {
	executedStmt *Statement
	sql.Result
}

func (e ExecResult) LastExecutedQuery() string {
	return e.executedStmt.lastExecutedQuery()
}
