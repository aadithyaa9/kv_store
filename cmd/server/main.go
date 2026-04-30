package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/aadithyaa9/kv_store/internal/config"
	"github.com/aadithyaa9/kv_store/internal/logger"
	"github.com/aadithyaa9/kv_store/internal/node"
	"github.com/aadithyaa9/kv_store/internal/transport"
)

func main() {
	logger.Init()
	cfg := config.Load()

	addr := "localhost:" + cfg.Port
	n := node.New(addr, cfg.Peers)

	handler := transport.NewHandler(n)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: handler,
	}

	go func() {
		log.Printf("Node running at %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop
	log.Println("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server.Shutdown(ctx)
}