package goredis

import (
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
)

type server struct {
	listener net.Listener
	logger   *slog.Logger

	started atomic.Bool
}

func NewServer(listener net.Listener, logger *slog.Logger) *server {
	return &server{
		listener: listener,
		logger:   logger,

		started: atomic.Bool{},
	}
}

func (s *server) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return fmt.Errorf("server already started")
	}

	s.logger.Info("server started")
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// TODO: handle shutdown.
			// at this point, the server might be shutting down.
			// or there is a network problem that causing the server can't accept new connections.
			// If it's the former, we just need to do some cleanup.
			// If it's the latter, we just need to return the error, nothing we can do anyway.
			break
		}

		go s.handleConn(conn)
	}

	return nil
}

func (s *server) Stop() error {
	if err := s.listener.Close(); err != nil {
		s.logger.Error(
			"cannot stop listener",
			slog.String("err", err.Error()),
		)
	}

	// Notice that we haven't close all remaining active connections here.
	// In Linux and in this particular case, it's technically necessary
	// because closing the listener will also close all remaining connections.
	// But, in some cases, we might need some kind of graceful shutdown logic
	// when closing the client.

	return nil
}

func (s *server) handleConn(conn net.Conn) {
	for {
		buff := make([]byte, 4096)
		n, err := conn.Read(buff)
		if err != nil {
			// TODO: handle the error
			break
		}

		if n == 0 {
			// TODO: this means the client is closing the connection
			break
		}

		if _, err := conn.Write(buff[:n]); err != nil {
			// TODO: handle the error
			break
		}
	}
}
