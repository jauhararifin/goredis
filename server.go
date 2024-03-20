package goredis

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jauhararifin/goredis/swisstable"
)

const nShard = 1000

type server struct {
	listener net.Listener
	logger   *slog.Logger

	started      atomic.Bool
	clients      map[int64]net.Conn
	lastClientId int64
	clientsLock  sync.Mutex
	shuttingDown bool

	dbLock   [nShard]sync.RWMutex
	database [nShard]*swisstable.Map
}

func NewServer(listener net.Listener, logger *slog.Logger) *server {
	s := &server{
		listener: listener,
		logger:   logger,

		started:      atomic.Bool{},
		clients:      make(map[int64]net.Conn, 100),
		lastClientId: 0,
		clientsLock:  sync.Mutex{},
		shuttingDown: false,

		dbLock:   [nShard]sync.RWMutex{},
		database: [nShard]*swisstable.Map{},
	}

	for i := 0; i < nShard; i++ {
		s.database[i] = swisstable.NewMap(1)
	}

	return s
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

	if !s.started.Load() {
		return fmt.Errorf("server not started yet")
	}
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

	reader := bufio.NewReader(conn)
	writer := newBufferWriter(conn)
	go func() {
		// TODO: handle shutdown for the buffered writer.
		if err := writer.Start(); err != nil {
			s.logger.Error(
				"buffered writer error",
				slog.Int64("id", clientId),
				slog.String("err", err.Error()),
			)
		}
	}()

	for {
		request, err := readArray(reader, true)
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

		if len(request) == 0 {
			s.logger.Error("missing command in the request", slog.Int64("clientId", clientId))
			break
		}

		commandName, ok := request[0].(string)
		if !ok {
			s.logger.Error("command is not a string", slog.Int64("clientId", clientId))
			break
		}

		switch strings.ToUpper(commandName) {
		case "GET":
			err = s.handleGetCommand(writer, request)
		case "SET":
			err = s.handleSetCommand(writer, request)
		default:
			s.logger.Error("unknown command", slog.String("command", commandName), slog.Int64("clientId", clientId))
			_, err = writer.Write([]byte("-ERR unknown command\r\n"))
			break
		}

		if err != nil {
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

func (s *server) handleGetCommand(conn io.Writer, command []any) error {
	if len(command) < 2 {
		_, err := conn.Write([]byte("-ERR missing key\r\n"))
		return err
	}

	key, ok := command[1].(string)
	if !ok {
		_, err := conn.Write([]byte("-ERR key is not a string\r\n"))
		return err
	}

	shard := calculateShard(key)
	s.dbLock[shard].RLock()
	value, ok := s.database[shard].Get(key)
	s.dbLock[shard].RUnlock()

	var err error
	if ok {
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
		_, err = conn.Write([]byte(resp))
	} else {
		_, err = conn.Write([]byte("_\r\n"))
	}
	return err
}

func calculateShard(s string) int {
	hasher := fnv.New64()
	_, _ = hasher.Write([]byte(s))
	hash := hasher.Sum64()
	return int(hash % uint64(nShard))
}

func (s *server) handleSetCommand(conn io.Writer, command []any) error {
	if len(command) < 3 {
		_, err := conn.Write([]byte("-ERR missing key and value\r\n"))
		return err
	}

	key, ok := command[1].(string)
	if !ok {
		_, err := conn.Write([]byte("-ERR key is not a string\r\n"))
		return err
	}

	value, ok := command[2].(string)
	if !ok {
		_, err := conn.Write([]byte("-ERR value is not a string\r\n"))
		return err
	}

	shard := calculateShard(key)
	s.dbLock[shard].Lock()
	s.database[shard].Put(key, value)
	s.dbLock[shard].Unlock()

	_, err := conn.Write([]byte("+OK\r\n"))
	return err
}
