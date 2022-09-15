package sqlbreaker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/chenquan/sqlbreaker/pkg/breaker"
	"github.com/stretchr/testify/assert"
)

func TestHook_BeginTx(t *testing.T) {
	breakerHook := NewBreakerHook(breaker.NewBreaker())
	checkWithContext(t, func(ctx context.Context) (context.Context, error) {
		ctx, _, err := breakerHook.BeforeBeginTx(ctx, driver.TxOptions{
			Isolation: 0,
			ReadOnly:  false,
		}, nil)
		return ctx, err
	}, func(ctx context.Context, err error) (context.Context, error) {
		ctx, _, err = breakerHook.AfterBeginTx(ctx, driver.TxOptions{}, nil, err)
		return ctx, err
	})
}

func TestHook_Connect(t *testing.T) {
	breakerHook := NewBreakerHook(breaker.NewBreaker())

	check(t, func(ctx context.Context) (context.Context, error) {
		ctx, err := breakerHook.BeforeConnect(ctx, nil)
		return ctx, err
	}, func(ctx context.Context) (context.Context, error) {
		ctx, _, err := breakerHook.AfterConnect(ctx, nil, nil)
		return ctx, err
	})
}

func TestHook_Commit(t *testing.T) {
	breakerHook := NewBreakerHook(breaker.NewBreaker())

	ctx, err := breakerHook.BeforeCommit(context.Background(), nil)
	assert.True(t, ctx == context.Background())
	assert.NoError(t, err)

	ctx, err = breakerHook.AfterCommit(context.Background(), nil)
	assert.True(t, ctx == context.Background())
	assert.NoError(t, err)
}

func TestHook_ExecContext(t *testing.T) {

	breakerHook := NewBreakerHook(breaker.NewBreaker())
	checkWithContext(t, func(ctx context.Context) (context.Context, error) {
		ctx, _, _, err := breakerHook.BeforeExecContext(ctx, "", nil, nil)
		return ctx, err
	}, func(ctx context.Context, err error) (context.Context, error) {
		ctx, _, err = breakerHook.AfterExecContext(ctx, "", nil, nil, err)
		return ctx, err
	})
}

func TestHook_PrepareContext(t *testing.T) {
	b := breaker.NewBreaker()
	breakerHook := NewBreakerHook(b)
	checkWithContext(t, func(ctx context.Context) (context.Context, error) {
		ctx, _, err := breakerHook.BeforePrepareContext(ctx, "", nil)

		return ctx, err
	}, func(ctx context.Context, err error) (context.Context, error) {
		ctx, _, err = breakerHook.AfterPrepareContext(ctx, "", nil, err)
		return ctx, err
	})
}

func TestHook_QueryContext(t *testing.T) {
	breakerHook := NewBreakerHook(breaker.NewBreaker())
	checkWithContext(t, func(ctx context.Context) (context.Context, error) {
		ctx, _, _, err := breakerHook.BeforeQueryContext(ctx, "", nil, nil)

		return ctx, err
	}, func(ctx context.Context, err error) (context.Context, error) {
		ctx, _, err = breakerHook.AfterQueryContext(ctx, "", nil, nil, err)
		return ctx, err
	})
}

func TestHook_Rollback(t *testing.T) {
	breakerHook := NewBreakerHook(breaker.NewBreaker())
	check(t, func(ctx context.Context) (context.Context, error) {
		ctx, err := breakerHook.BeforeRollback(ctx, nil)
		return ctx, err
	}, func(ctx context.Context) (context.Context, error) {
		ctx, err := breakerHook.AfterRollback(ctx, nil)
		return ctx, err
	})
}

func TestHook_StmtExecContext(t *testing.T) {
	breakerHook := NewBreakerHook(breaker.NewBreaker())
	checkWithContext(t, func(ctx context.Context) (context.Context, error) {
		ctx, _, err := breakerHook.BeforeStmtExecContext(ctx, "", nil, nil)
		return ctx, err
	}, func(ctx context.Context, err error) (context.Context, error) {
		ctx, _, err = breakerHook.AfterStmtExecContext(ctx, "", nil, nil, err)
		return ctx, err
	})
}

func TestHook_StmtQueryContext(t *testing.T) {
	breakerHook := NewBreakerHook(breaker.NewBreaker())
	checkWithContext(t, func(ctx context.Context) (context.Context, error) {
		ctx, _, err := breakerHook.BeforeStmtQueryContext(ctx, "", nil, nil)
		return ctx, err
	}, func(ctx context.Context, err error) (context.Context, error) {
		ctx, _, err = breakerHook.AfterStmtQueryContext(ctx, "", nil, nil, err)
		return ctx, err
	})
}

func checkWithContext(t *testing.T, before func(ctx context.Context) (context.Context, error), after func(ctx context.Context, err error) (context.Context, error)) {
	t.Run("allow", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			ctx, err := before(context.Background())

			assert.True(t, ctx.Value(allowKey{}) != nil)
			assert.NoError(t, err)

			if i%2 == 0 {
				ctx, err = after(ctx, nil)
				assert.NoError(t, err)
			} else {
				ctx, err = after(ctx, sql.ErrNoRows)
				assert.ErrorIs(t, err, sql.ErrNoRows)
			}

			assert.True(t, ctx.Value(allowKey{}) != nil)
		}
	})

	t.Run("not allowed", func(t *testing.T) {
		b := breaker.NewBreaker()
		breakerHook := NewBreakerHook(b)

		openBreaker := false
		for i := 0; i < 1000; i++ {
			ctx, _, _, err := breakerHook.BeforeExecContext(context.Background(), "", nil, nil)

			if err == breaker.ErrServiceUnavailable {
				openBreaker = true
				assert.True(t, ctx.Value(allowKey{}) == nil)
			} else {
				assert.True(t, ctx.Value(allowKey{}) != nil)
			}

			_, _, err = breakerHook.AfterExecContext(ctx, "", nil, nil, errors.New("any"))
			assert.Error(t, err)
		}

		assert.True(t, openBreaker)
	})
}

func check(t *testing.T, before, after func(ctx context.Context) (context.Context, error)) {

	ctx, err := before(context.Background())
	assert.True(t, ctx == context.Background())
	assert.NoError(t, err)

	ctx, err = after(ctx)
	assert.True(t, ctx == context.Background())
	assert.NoError(t, err)
}
