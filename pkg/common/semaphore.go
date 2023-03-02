package common

import "go.uber.org/ratelimit"

type Semaphore struct {
	pool chan struct{}
	size int

	limiter    ratelimit.Limiter
	useLimiter bool
}

type OptionFunc func(*Semaphore)

func WithLimiter(size int) OptionFunc {
	return func(sem *Semaphore) {
		sem.limiter = ratelimit.New(size)
		sem.useLimiter = true
	}
}

func NewSemaphore(size int, opts ...OptionFunc) *Semaphore {
	sem := &Semaphore{
		pool: make(chan struct{}, size),
		size: size,
	}

	for _, opt := range opts {
		opt(sem)
	}
	return sem
}

func (s *Semaphore) Acquire() {
	if s.useLimiter {
		s.limiter.Take()
	}
	s.pool <- struct{}{}
}

func (s *Semaphore) Release() {
	<-s.pool
}
