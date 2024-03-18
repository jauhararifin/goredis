package goredis

import (
	"log/slog"
	"net"
)

type server struct {
	listener net.Listener
	logger   *slog.Logger
}

func NewServer(listener net.Listener, logger *slog.Logger) *server {
	return &server{
		listener: listener,
		logger:   logger,
	}
}

func (s *server) Start() error {
	panic("todo")
}

func (s *server) Stop() error {
	panic("todo")
}
