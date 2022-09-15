package sqlbreaker

import (
	"database/sql/driver"
	"testing"

	"github.com/chenquan/sqlbreaker/pkg/breaker"
	"github.com/stretchr/testify/assert"
)

func TestNewDefaultDriver(t *testing.T) {
	assert.NotNil(t, NewDefaultDriver(driver.Driver(nil)))
}

func TestNewDriver(t *testing.T) {
	assert.NotNil(t, NewDriver(breaker.Breaker(nil), driver.Driver(nil)))
}

func TestNewBreakerHook(t *testing.T) {
	hook := NewBreakerHook(breaker.Breaker(nil))
	assert.Equal(t, &Hook{brk: breaker.Breaker(nil)}, hook)
}
