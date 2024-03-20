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

	client, err := net.Dial("unix", sockfilePath)
	if err != nil {
		t.Errorf("cannot open sock file: %s", err.Error())
		return
	}

	if _, err := client.Write([]byte("*3\r\n$3\r\nset\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")); err != nil {
		t.Errorf("cannot send 'SET key1 value1' to server: %s", err.Error())
		return
	}

	buff := make([]byte, 4096)
	nRead, err := io.ReadFull(client, buff[:5])
	if err != nil {
		t.Errorf("cannot read server response: %s", err.Error())
		return
	}

	if string(buff[:nRead]) != "+OK\r\n" {
		t.Errorf("calling SET should return 'OK' response, but istead it returns '%s'", string(buff[:nRead]))
		return
	}

	if _, err := client.Write([]byte("*2\r\n$3\r\nget\r\n$4\r\nkey1\r\n")); err != nil {
		t.Errorf("cannot send 'GET key1' to server: %s", err.Error())
		return
	}

	nRead, err = io.ReadFull(client, buff[:12])
	if err != nil {
		t.Errorf("cannot read server response: %s", err.Error())
		return
	}

	if string(buff[:nRead]) != "$6\r\nvalue1\r\n" {
		t.Errorf("calling GET should return 'value1' response, but istead it returns '%s'", string(buff[:nRead]))
		return
	}
}
