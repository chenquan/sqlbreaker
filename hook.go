package sqlbreaker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/chenquan/sqlbreaker/pkg/breaker"
	"github.com/chenquan/sqlplus"
)

var _ sqlplus.Hook = (*Hook)(nil)

type (
	Hook struct {
		brk breaker.Breaker
	}
	allowKey struct{}
)

func NewBreakerHook(brk breaker.Breaker) *Hook {
	return &Hook{brk: brk}
}

func (h *Hook) BeforeClose(ctx context.Context, err error) (context.Context, error) {
	return ctx, err
}

func (h *Hook) AfterClose(ctx context.Context, err error) (context.Context, error) {
	return ctx, err
}

func (h *Hook) BeforeConnect(ctx context.Context, err error) (context.Context, error) {
	return ctx, err
}

func (h *Hook) AfterConnect(ctx context.Context, dc driver.Conn, err error) (context.Context, driver.Conn, error) {
	return ctx, dc, err
}

func (h *Hook) BeforeExecContext(ctx context.Context, query string, args []driver.NamedValue, _ error) (context.Context, string, []driver.NamedValue, error) {
	ctx, err := h.allow(ctx)

	return ctx, query, args, err
}

func (h *Hook) AfterExecContext(ctx context.Context, _ string, _ []driver.NamedValue, dr driver.Result, err error) (context.Context, driver.Result, error) {
	h.handleAllow(ctx, err)

	return ctx, dr, err
}

func (h *Hook) BeforeBeginTx(ctx context.Context, opts driver.TxOptions, _ error) (context.Context, driver.TxOptions, error) {
	ctx, err := h.allow(ctx)

	return ctx, opts, err
}

func (h *Hook) AfterBeginTx(ctx context.Context, _ driver.TxOptions, dt driver.Tx, err error) (context.Context, driver.Tx, error) {
	h.handleAllow(ctx, err)

	return ctx, dt, err
}

func (h *Hook) BeforeQueryContext(ctx context.Context, query string, args []driver.NamedValue, _ error) (context.Context, string, []driver.NamedValue, error) {
	ctx, err := h.allow(ctx)

	return ctx, query, args, err
}

func (h *Hook) AfterQueryContext(ctx context.Context, _ string, _ []driver.NamedValue, rows driver.Rows, err error) (context.Context, driver.Rows, error) {
	h.handleAllow(ctx, err)

	return ctx, rows, err
}

func (h *Hook) BeforePrepareContext(ctx context.Context, query string, _ error) (context.Context, string, error) {
	ctx, err := h.allow(ctx)

	return ctx, query, err
}

func (h *Hook) AfterPrepareContext(ctx context.Context, _ string, ds driver.Stmt, err error) (context.Context, driver.Stmt, error) {
	h.handleAllow(ctx, err)

	return ctx, ds, err
}

func (h *Hook) BeforeCommit(ctx context.Context, err error) (context.Context, error) {
	return ctx, err
}

func (h *Hook) AfterCommit(ctx context.Context, err error) (context.Context, error) {
	return ctx, err
}

func (h *Hook) BeforeRollback(ctx context.Context, err error) (context.Context, error) {
	return ctx, err
}

func (h *Hook) AfterRollback(ctx context.Context, err error) (context.Context, error) {
	return ctx, err
}

func (h *Hook) BeforeStmtQueryContext(ctx context.Context, _ string, args []driver.NamedValue, _ error) (context.Context, []driver.NamedValue, error) {
	ctx, err := h.allow(ctx)

	return ctx, args, err
}

func (h *Hook) AfterStmtQueryContext(ctx context.Context, _ string, _ []driver.NamedValue, rows driver.Rows, err error) (context.Context, driver.Rows, error) {
	h.handleAllow(ctx, err)

	return ctx, rows, err
}

func (h *Hook) BeforeStmtExecContext(ctx context.Context, _ string, args []driver.NamedValue, _ error) (context.Context, []driver.NamedValue, error) {
	ctx, err := h.allow(ctx)

	return ctx, args, err
}

func (h *Hook) AfterStmtExecContext(ctx context.Context, _ string, _ []driver.NamedValue, r driver.Result, err error) (context.Context, driver.Result, error) {
	h.handleAllow(ctx, err)

	return ctx, r, err
}

func (h *Hook) allow(ctx context.Context) (context.Context, error) {
	allow, err := h.brk.Allow()
	if err != nil {
		return ctx, err
	}
	ctx = context.WithValue(ctx, allowKey{}, allow)

	return ctx, err
}

func (h *Hook) handleAllow(ctx context.Context, err error) {
	value := ctx.Value(allowKey{})
	if value == nil {
		return
	}

	allow := value.(breaker.Promise)
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		allow.Accept()
		return
	}

	allow.Reject(err.Error())
}
