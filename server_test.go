package goredis

import (
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
)

const sockfilePath string = "/tmp/redisexperiment.sock"

func TestServerBootstrap(t *testing.T) {
	_ = os.Remove(sockfilePath)
	listener, err := net.Listen("unix", sockfilePath)
	if err != nil {
		t.Errorf("cannot open sock file: %s", err.Error())
		return
	}

	noopLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewServer(listener, noopLogger)

	if err = server.Stop(); err == nil {
		t.Error("stopping server before it's started should return an error")
		return
	}

	// Starting the server 10 times.
	// By right, only one of them should success, and the rest should return
	// an error
	n := 10
	startErrorChan := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			err := server.Start()
			startErrorChan <- err
		}()
	}

	for i := 0; i < n-1; i++ {
		err := <-startErrorChan
		if err == nil {
			t.Error("there should be N-1 invocation of `Start` that returns error")
			return
		}
	}
}
