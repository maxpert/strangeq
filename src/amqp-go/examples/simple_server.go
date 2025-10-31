//go:build ignore

package main

import (
	"log"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"go.uber.org/zap"
)

func main() {
	// Create a new AMQP server
	srv := server.NewServer(":5672")

	// Start the server
	log.Println("Starting AMQP server on :5672...")
	if err := srv.Start(); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

// Example with custom logger
func exampleWithCustomLogger() {
	// Create a custom logger
	logger, _ := zap.NewDevelopment()

	// Create server with custom configuration
	srv := &server.Server{
		Addr:        ":5672",
		Connections: make(map[string]*protocol.Connection),
		Log:         logger,
		Broker:      server.NewOriginalBrokerAdapter(broker.NewBroker()),
	}

	// Start the server
	log.Println("Starting AMQP server with custom logger...")
	if err := srv.Start(); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}
