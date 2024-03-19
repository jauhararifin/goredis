package goredis

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
)

type server struct {
	listener net.Listener
	logger   *slog.Logger

	started      atomic.Bool
	clients      map[int64]net.Conn
	lastClientId int64
	clientsLock  sync.Mutex
	shuttingDown bool
}

func NewServer(listener net.Listener, logger *slog.Logger) *server {
	return &server{
		listener: listener,
		logger:   logger,

		started:      atomic.Bool{},
		clients:      make(map[int64]net.Conn, 100),
		lastClientId: 0,
		clientsLock:  sync.Mutex{},
		shuttingDown: false,
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
			s.clientsLock.Lock()
			isShuttingDown := s.shuttingDown
			s.clientsLock.Unlock()

			if !isShuttingDown {
				return err
			}
			return nil
		}

		s.clientsLock.Lock()
		s.lastClientId += 1
		clientId := s.lastClientId
		s.clients[clientId] = conn
		s.clientsLock.Unlock()
		go s.handleConn(clientId, conn)
	}
}

func (s *server) Stop() error {
	s.clientsLock.Lock()
	defer s.clientsLock.Unlock()

	if s.shuttingDown {
		return fmt.Errorf("already shutting down")
	}
	s.shuttingDown = true

	for clientId, conn := range s.clients {
		s.logger.Info(
			"closing client",
			slog.Int64("clientId", clientId),
		)
		if err := conn.Close(); err != nil {
			s.logger.Error(
				"cannot close client",
				slog.Int64("clientId", clientId),
				slog.String("err", err.Error()),
			)
		}
	}
	clear(s.clients)

	if err := s.listener.Close(); err != nil {
		s.logger.Error(
			"cannot stop listener",
			slog.String("err", err.Error()),
		)
	}

	return nil
}

func (s *server) handleConn(clientId int64, conn net.Conn) {
	s.logger.Info(
		"client connected",
		slog.Int64("id", clientId),
		slog.String("host", conn.RemoteAddr().String()),
	)

	for {
		buff := make([]byte, 4096)
		n, err := conn.Read(buff)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.logger.Error(
					"error reading from client",
					slog.Int64("clientId", clientId),
					slog.String("err", err.Error()),
				)
			}

			break
		}

		if n == 0 {
			break
		}

		if _, err := conn.Write(buff[:n]); err != nil {
			s.logger.Error(
				"error writing to client",
				slog.Int64("clientId", clientId),
				slog.String("err", err.Error()),
			)
			break
		}
	}

	s.clientsLock.Lock()
	if _, ok := s.clients[clientId]; !ok {
		s.clientsLock.Unlock()
		return
	}
	delete(s.clients, clientId)
	s.clientsLock.Unlock()

	s.logger.Info("client disconnecting", slog.Int64("clientId", clientId))
	if err := conn.Close(); err != nil {
		s.logger.Error(
			"cannot close client",
			slog.Int64("clientId", clientId),
			slog.String("err", err.Error()),
		)
	}
}
