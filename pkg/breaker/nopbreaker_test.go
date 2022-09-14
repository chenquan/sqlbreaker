package breaker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNopBreaker(t *testing.T) {
	b := newNoOpBreaker()
	assert.Equal(t, noOpBreakerName, b.Name())
	p, err := b.Allow()
	assert.Nil(t, err)
	p.Accept()
	for i := 0; i < 1000; i++ {
		p, err := b.Allow()
		assert.Nil(t, err)
		p.Reject("any")
	}
}
