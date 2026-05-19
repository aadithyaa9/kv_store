package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"kvstore/internal/config"
	"kvstore/internal/logger"
	"kvstore/internal/node"
	"kvstore/internal/transport"
)

func main() {
	logger.Init()
	cfg := config.Load()

	addr := "localhost:" + cfg.Port
	n := node.New(addr, cfg.Peers, cfg.VNodes)

	handler := transport.NewHandler(n)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: handler,
	}

	go func() {
		log.Printf("Node running at %s | vnodes=%d | peers=%v", addr, cfg.VNodes, cfg.Peers)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Block until Ctrl+C
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	log.Println("Shutting down gracefully...")

	// ── Graceful shutdown sequence ──────────────────────────────────────────
	// Step 1: Remove self from ring (so new requests route elsewhere)
	n.RemoveNode(n.Addr)

	// Step 2: Migrate all locally owned keys to their new ring-assigned owners
	n.MigrateKeys()

	// Step 3: Stop accepting new connections, wait for in-flight to finish
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)

	log.Println("Shutdown complete")
}
