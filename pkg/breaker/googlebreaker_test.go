package breaker

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/chenquan/sqlbreaker/pkg/collection"
	"github.com/chenquan/sqlbreaker/pkg/mathx"
	"github.com/stretchr/testify/assert"
)

const (
	testBuckets  = 10
	testInterval = time.Millisecond * 10
)

func getGoogleBreaker() *googleBreaker {
	st := collection.NewRollingWindow(testBuckets, testInterval)
	return &googleBreaker{
		stat:  st,
		k:     5,
		proba: mathx.NewProba(),
	}
}

func markSuccessWithDuration(b *googleBreaker, count int, sleep time.Duration) {
	for i := 0; i < count; i++ {
		b.markSuccess()
		time.Sleep(sleep)
	}
}

func markFailedWithDuration(b *googleBreaker, count int, sleep time.Duration) {
	for i := 0; i < count; i++ {
		b.markFailure()
		time.Sleep(sleep)
	}
}

func TestGoogleBreakerClose(t *testing.T) {
	b := getGoogleBreaker()
	markSuccess(b, 80)
	assert.Nil(t, b.accept())
	markSuccess(b, 120)
	assert.Nil(t, b.accept())
}

func TestGoogleBreakerOpen(t *testing.T) {
	b := getGoogleBreaker()
	markSuccess(b, 10)
	assert.Nil(t, b.accept())
	markFailed(b, 100000)
	time.Sleep(testInterval * 2)
	verify(t, func() bool {
		return b.accept() != nil
	})
}

func TestGoogleBreakerHalfOpen(t *testing.T) {
	b := getGoogleBreaker()
	assert.Nil(t, b.accept())
	t.Run("accept single failed/accept", func(t *testing.T) {
		markFailed(b, 10000)
		time.Sleep(testInterval * 2)
		verify(t, func() bool {
			return b.accept() != nil
		})
	})
	t.Run("accept single failed/allow", func(t *testing.T) {
		markFailed(b, 10000)
		time.Sleep(testInterval * 2)
		verify(t, func() bool {
			_, err := b.allow()
			return err != nil
		})
	})
	time.Sleep(testInterval * testBuckets)
	t.Run("accept single succeed", func(t *testing.T) {
		assert.Nil(t, b.accept())
		markSuccess(b, 10000)
		verify(t, func() bool {
			return b.accept() == nil
		})
	})
}

func TestGoogleBreakerSelfProtection(t *testing.T) {
	t.Run("total request < 100", func(t *testing.T) {
		b := getGoogleBreaker()
		markFailed(b, 4)
		time.Sleep(testInterval)
		assert.Nil(t, b.accept())
	})
	t.Run("total request > 100, total < 2 * success", func(t *testing.T) {
		b := getGoogleBreaker()
		size := rand.Intn(10000)
		accepts := size + 1
		markSuccess(b, accepts)
		markFailed(b, size-accepts)
		assert.Nil(t, b.accept())
	})
}

func TestGoogleBreakerHistory(t *testing.T) {
	var b *googleBreaker
	var accepts, total int64

	sleep := testInterval
	t.Run("accepts == total", func(t *testing.T) {
		b = getGoogleBreaker()
		markSuccessWithDuration(b, 10, sleep/2)
		accepts, total = b.history()
		assert.Equal(t, int64(10), accepts)
		assert.Equal(t, int64(10), total)
	})

	t.Run("fail == total", func(t *testing.T) {
		b = getGoogleBreaker()
		markFailedWithDuration(b, 10, sleep/2)
		accepts, total = b.history()
		assert.Equal(t, int64(0), accepts)
		assert.Equal(t, int64(10), total)
	})

	t.Run("accepts = 1/2 * total, fail = 1/2 * total", func(t *testing.T) {
		b = getGoogleBreaker()
		markFailedWithDuration(b, 5, sleep/2)
		markSuccessWithDuration(b, 5, sleep/2)
		accepts, total = b.history()
		assert.Equal(t, int64(5), accepts)
		assert.Equal(t, int64(10), total)
	})

	t.Run("auto reset rolling counter", func(t *testing.T) {
		b = getGoogleBreaker()
		time.Sleep(testInterval * testBuckets)
		accepts, total = b.history()
		assert.Equal(t, int64(0), accepts)
		assert.Equal(t, int64(0), total)
	})
}

func BenchmarkGoogleBreakerAllow(b *testing.B) {
	breaker := getGoogleBreaker()
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		breaker.accept()
		if i%2 == 0 {
			breaker.markSuccess()
		} else {
			breaker.markFailure()
		}
	}
}

func markSuccess(b *googleBreaker, count int) {
	for i := 0; i < count; i++ {
		p, err := b.allow()
		if err != nil {
			break
		}
		p.Accept()
	}
}

func markFailed(b *googleBreaker, count int) {
	for i := 0; i < count; i++ {
		p, err := b.allow()
		if err == nil {
			p.Reject()
		}
	}
}

func verify(t *testing.T, fn func() bool) {
	var count int
	for i := 0; i < 100; i++ {
		if fn() {
			count++
		}
	}
	assert.True(t, count >= 80, fmt.Sprintf("should be greater than 80, actual %d", count))
}
