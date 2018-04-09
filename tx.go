package gdo

import (
	"context"
	"database/sql"
)

type Transaction struct {
	*sql.Tx
}

func (tx Transaction) Exec(s *Statement) (ExecResult, error) {
	return tx.ExecContext(context.Background(), s)
}

func (tx Transaction) ExecContext(ctx context.Context, s *Statement) (ExecResult, error) {
	return doExecCtx(tx.Tx.ExecContext, ctx, s)
}

func (tx Transaction) Query(s *Statement) (QueryResult, error) {
	return tx.QueryContext(context.Background(), s)
}

func (tx Transaction) QueryContext(ctx context.Context, s *Statement) (QueryResult, error) {
	return doQueryCtx(tx.Tx.QueryContext, ctx, s)
}
