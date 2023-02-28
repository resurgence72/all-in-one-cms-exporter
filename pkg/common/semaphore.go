package common

type Semaphore struct {
	pool chan struct{}
	size int
}

func NewSemaphore(size int) *Semaphore {
	return &Semaphore{
		pool: make(chan struct{}, size),
		size: size,
	}
}

func (s *Semaphore) Acquire() {
	s.pool <- struct{}{}
}

func (s *Semaphore) Release() {
	<-s.pool
}
