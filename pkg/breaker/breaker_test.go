package breaker

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker_Allow(t *testing.T) {
	t.Run("allow", func(t *testing.T) {
		b := NewBreaker(WithName("any"))
		assert.True(t, len(b.Name()) > 0)
		for i := 0; i < 1000; i++ {
			allow, err := b.Allow()
			assert.Nil(t, err)
			allow.Accept()
		}
	})

	t.Run("not allowed", func(t *testing.T) {
		b := NewBreaker()
		assert.True(t, len(b.Name()) > 0)
		openBreaker := false
		for i := 0; i < 1000; i++ {
			allow, err := b.Allow()
			if err == ErrServiceUnavailable {
				openBreaker = true
			} else {
				allow.Reject("any")
				assert.Nil(t, err)
			}
		}

		assert.True(t, openBreaker)
	})
}

func TestErrorWindow(t *testing.T) {
	tests := []struct {
		name    string
		reasons []string
	}{
		{
			name: "no error",
		},
		{
			name:    "one error",
			reasons: []string{"foo"},
		},
		{
			name:    "two errors",
			reasons: []string{"foo", "bar"},
		},
		{
			name:    "five errors",
			reasons: []string{"first", "second", "third", "fourth", "fifth"},
		},
		{
			name:    "six errors",
			reasons: []string{"first", "second", "third", "fourth", "fifth", "sixth"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var ew errorWindow
			for _, reason := range test.reasons {
				ew.add(reason)
			}
			var reasons []string
			if len(test.reasons) > numHistoryReasons {
				reasons = test.reasons[len(test.reasons)-numHistoryReasons:]
			} else {
				reasons = test.reasons
			}
			for _, reason := range reasons {
				assert.True(t, strings.Contains(ew.String(), reason), fmt.Sprintf("actual: %s", ew.String()))
			}
		})
	}
}

func TestPromiseWithReason(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		expect string
	}{
		{
			name: "success",
		},
		{
			name:   "success",
			reason: "fail",
			expect: "fail",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			promise := promiseWithReason{
				promise: new(mockedPromise),
				errWin:  new(errorWindow),
			}
			if len(test.reason) == 0 {
				promise.Accept()
			} else {
				promise.Reject(test.reason)
			}

			assert.True(t, strings.Contains(promise.errWin.String(), test.expect))
		})
	}
}

type mockedPromise struct{}

func (m *mockedPromise) Accept() {
}

func (m *mockedPromise) Reject() {
}
