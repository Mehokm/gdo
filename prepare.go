package gdo

import "database/sql"

type PreparedStatement struct {
	s *Statement
	*sql.Stmt
}

// TODO: finish implementation
