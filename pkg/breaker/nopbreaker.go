package breaker

const noOpBreakerName = "nopBreaker"

type noOpBreaker struct{}

func newNoOpBreaker() Breaker {
	return noOpBreaker{}
}

func (b noOpBreaker) Name() string {
	return noOpBreakerName
}

func (b noOpBreaker) Allow() (Promise, error) {
	return NopPromise{}, nil
}

type NopPromise struct{}

func (p NopPromise) Accept() {
}

func (p NopPromise) Reject(reason string) {
}
