//go:build ignore

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/server"
)

func main() {
	// Create configuration
	cfg := config.DefaultConfig()
	cfg.Network.Address = ":5672"

	// Create metrics collector
	metricsCollector := metrics.NewCollector("amqp")

	// Create metrics HTTP server (port 9419 - standard AMQP exporter port)
	metricsServer := metrics.NewServer(9419)

	// Start metrics server in background
	go func() {
		log.Printf("Starting Prometheus metrics server on :%d", metricsServer.Port())
		log.Printf("Metrics available at http://localhost:%d/metrics", metricsServer.Port())
		if err := metricsServer.Start(); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// Create AMQP server
	amqpServer := server.NewServer(cfg.Network.Address)
	amqpServer.Config = cfg
	amqpServer.MetricsCollector = metricsCollector

	// Start background goroutine to update server uptime
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				uptime := time.Since(amqpServer.StartTime).Seconds()
				metricsCollector.UpdateServerUptime(uptime)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start AMQP server in background
	go func() {
		log.Printf("Starting AMQP server on %s", cfg.Network.Address)
		if err := amqpServer.Start(); err != nil {
			log.Fatalf("AMQP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sig := <-sigChan
	log.Printf("Received signal: %v, shutting down...", sig)

	// Shutdown metrics server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := metricsServer.Stop(shutdownCtx); err != nil {
		log.Printf("Error stopping metrics server: %v", err)
	}

	// Shutdown AMQP server
	amqpServer.Mutex.Lock()
	amqpServer.Shutdown = true
	amqpServer.Mutex.Unlock()

	if amqpServer.Listener != nil {
		amqpServer.Listener.Close()
	}

	log.Println("Server shutdown complete")
}
