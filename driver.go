package sqlbreaker

import (
	"database/sql/driver"

	"github.com/chenquan/sqlbreaker/pkg/breaker"
	"github.com/chenquan/sqlplus"
)

func NewDriver(b breaker.Breaker, d driver.Driver) driver.Driver {
	return sqlplus.New(d, NewBreakerHook(b))
}

func NewDefaultDriver(d driver.Driver) driver.Driver {
	return sqlplus.New(d, NewBreakerHook(breaker.NewBreaker()))
}
