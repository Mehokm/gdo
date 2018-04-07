package gdo

import "database/sql"

type ExecResult struct {
	ExecutedQuery string
	sql.Result
}
