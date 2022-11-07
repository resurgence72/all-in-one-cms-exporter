package common

type semaphore struct {
	pool chan struct{}
	size int
}

func Semaphore(size int) *semaphore {
	return &semaphore{
		pool: make(chan struct{}, size),
		size: size,
	}
}

func (s *semaphore) Acquire() {
	s.pool <- struct{}{}
}

func (s *semaphore) Release() {
	<-s.pool
}
