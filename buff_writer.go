package goredis

import (
	"io"
	"sync"
	"time"
)

type bufferWriter struct {
	w    io.Writer
	lock *sync.Mutex
	buff []byte
	n    int
	err  error
}

func newBufferWriter(w io.Writer) *bufferWriter {
	lock := &sync.Mutex{}
	return &bufferWriter{
		w:    w,
		lock: lock,
		buff: make([]byte, 4096),
		n:    0,
		err:  nil,
	}
}

func (b *bufferWriter) Start() error {
	// TODO: Handle shutdown logic.
	// this implementation doesn't handle shutdown logic.
	// Because of that, this implementation leaks memory and
	// goroutines.

	for {
		// TODO: avoid cycle when there is no available bytes.
		// This implementation will periodically try flushing
		// the available bytes. When the client is idle, this
		// implementation will still loop periodically.
		// This is wasting out CPU. To improve this, we can
		// use sync.Cond to sleep when there is no available
		// byte, and when someone fill the buffer, we can
		// wake up.
		b.lock.Lock()
		if b.n == 0 {
			b.lock.Unlock()
			time.Sleep(200 * time.Microsecond)
			continue
		}

		if err := b.flush(); err != nil {
			b.lock.Unlock()
			return err
		}
		b.lock.Unlock()
	}
}

func (b *bufferWriter) Write(buff []byte) (int, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	totalWrite := 0

	for len(buff) > len(b.buff)-b.n && b.err == nil {
		var n int
		if b.n == 0 {
			n, b.err = b.w.Write(buff)
		} else {
			n = copy(b.buff[b.n:], buff)
			b.n += n
			b.err = b.flush()
		}
		totalWrite += n
		buff = buff[n:]
	}

	if b.err != nil {
		return totalWrite, b.err
	}

	n := copy(b.buff[b.n:], buff)
	b.n += n
	totalWrite += n

	return totalWrite, nil
}

// lock invariant: b.lock should be acquired
func (b *bufferWriter) flush() error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}

	n, err := b.w.Write(b.buff[:b.n])
	if n < b.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < b.n {
			copy(b.buff[0:b.n-n], b.buff[n:b.n])
		}
		b.n -= n
		b.err = err
		return err
	}

	b.n = 0
	return nil
}
