package goredis

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"
)

const sockfilePath string = "/tmp/redisexperiment.sock"

func TestServerBootstrap(t *testing.T) {
	timer := time.AfterFunc(5*time.Second, func() {
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Errorf("test runs too long, something is wrong")
		t.FailNow()
	})
	defer timer.Stop()

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

	// Stopping the server 10 times
	// By right, only one of them should success and the rest should return aerrors.
	stopErrorChan := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			err := server.Stop()
			stopErrorChan <- err
		}()
	}

	nSuccess := 0
	nError := 0
	for i := 0; i < n; i++ {
		err := <-stopErrorChan
		if err != nil {
			nError++
		} else {
			nSuccess++
		}
	}

	if nSuccess != 1 {
		t.Error("there should exactly one invocation of `Stop` that succeded")
		return
	}
	if nError != n-1 {
		t.Error("there should exactly n-1 invocation of `Stop` that fails")
		return
	}

	if err = <-startErrorChan; err != nil {
		t.Error("after stopped, the first call to `Start` should return nil")
		return
	}

	if _, err := client.Read(buff); err == nil || !errors.Is(err, io.EOF) {
		t.Error("after stop complete, all client should be disconnected")
		return
	}
}

// Initial benchmark:
// goos: linux
// goarch: amd64
// pkg: github.com/jauhararifin/goredis
// cpu: AMD Ryzen 9 7900X 12-Core Processor
// BenchmarkRedisSet
// BenchmarkRedisSet-24             9213043              1349 ns/op            741066 ops/sec           593 B/op         42 allocs/op
//
// After adding buffered reader:
// goos: linux
// goarch: amd64
// pkg: github.com/jauhararifin/goredis
// cpu: AMD Ryzen 9 7900X 12-Core Processor
// BenchmarkRedisSet
// BenchmarkRedisSet-24            11445344               928.4 ns/op         1077079 ops/sec           568 B/op         42 allocs/op
//
// After using buffered writer:
// goos: linux
// goarch: amd64
// pkg: github.com/jauhararifin/goredis
// cpu: AMD Ryzen 9 7900X 12-Core Processor
// BenchmarkRedisSet
// BenchmarkRedisSet-24            12226862               929.9 ns/op         1075353 ops/sec           563 B/op         42 allocs/op
//
// After using better buffered writer:
// goos: linux
// goarch: amd64
// pkg: github.com/jauhararifin/goredis
// cpu: AMD Ryzen 9 7900X 12-Core Processor
// BenchmarkRedisSet
// BenchmarkRedisSet-24            15214984               864.4 ns/op         1156883 ops/sec           622 B/op         42 allocs/op
func BenchmarkRedisSet(b *testing.B) {
	_ = os.Remove(sockfilePath)
	listener, err := net.Listen("unix", sockfilePath)
	if err != nil {
		b.Errorf("cannot open sock file: %s", err.Error())
		return
	}

	noopLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewServer(listener, noopLogger)

	go func() {
		if err := server.Start(); err != nil {
			b.Errorf("cannot start server: %s", err.Error())
		}
	}()

	b.ResetTimer()

	id := atomic.Int64{}
	b.RunParallel(func(pb *testing.PB) {
		client, err := net.Dial("unix", sockfilePath)
		if err != nil {
			b.Errorf("cannot connect to server: %s", err.Error())
			b.FailNow()
			return
		}

		randomizer := rand.New(rand.NewSource(id.Add(1)))

		pipelineSize := 100

		buff := make([]byte, 4096)
		writeBuffer := bytes.Buffer{}
		count := 0

		for pb.Next() {
			writeBuffer.WriteString("*3\r\n$3\r\nset\r\n$12\r\n")
			for i := 0; i < 12; i++ {
				writeBuffer.WriteByte(byte(randomizer.Int31()%96 + 32))
			}
			writeBuffer.WriteString("\r\n$12\r\n")
			for i := 0; i < 12; i++ {
				writeBuffer.WriteByte(byte(randomizer.Int31()%96 + 32))
			}
			writeBuffer.WriteString("\r\n")
			count++

			if count >= pipelineSize {
				if _, err := writeBuffer.WriteTo(client); err != nil {
					b.Errorf("cannot write to server: %s", err.Error())
					return
				}

				if _, err := io.ReadFull(client, buff[:5*count]); err != nil {
					b.Errorf("cannot read from server: %s", err.Error())
					return
				}

				count = 0
			}
		}

		if count > 0 {
			if _, err := writeBuffer.WriteTo(client); err != nil {
				b.Errorf("cannot write to server: %s", err.Error())
				return
			}

			if _, err := io.ReadFull(client, buff[:5*count]); err != nil {
				b.Errorf("cannot read from server: %s", err.Error())
				return
			}

			count = 0
		}

		if err := client.Close(); err != nil {
			b.Errorf("cannot close client: %s", err.Error())
			return
		}
	})
	b.StopTimer()

	if err := server.Stop(); err != nil {
		b.Errorf("cannot stop server: %s", err.Error())
		return
	}

	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "ops/sec")
}
