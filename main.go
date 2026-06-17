package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/mujib77/rift/internal/config"
	"github.com/mujib77/rift/internal/engine"
)

func main() {
	configPath := "rift.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Println("error loading config:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		fmt.Println("\n  signal received, shutting down...")
		cancel()
	}()

	eng, err := engine.New(cfg)
	if err != nil {
		fmt.Println("error creating engine:", err)
		os.Exit(1)
	}

	if err := eng.Start(ctx); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}