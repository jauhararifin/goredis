package main

import (
	"log/slog"
	"net"
	"os"
	"os/signal"

	"github.com/jauhararifin/goredis"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	address := "0.0.0.0:3100"
	logger.Info("starting server", slog.String("address", address))

	listener, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error(
			"cannot start tcp server",
			slog.String("address", address),
			slog.String("err", err.Error()),
		)
		os.Exit(-1)
	}

	server := goredis.NewServer(listener, logger)
	go func() {
		if err := server.Start(); err != nil {
			logger.Error("server error", slog.String("err", err.Error()))
			os.Exit(1)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	if err := server.Stop(); err != nil {
		logger.Error("canot stop server", slog.String("err", err.Error()))
		os.Exit(1)
	}
}
