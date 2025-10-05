package main

import (
	"fmt"
	"log"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"github.com/maxpert/amqp-go/transaction"
)

// TransactionCompleteDemo demonstrates the complete transaction system with broker setup
func main() {
	fmt.Println("AMQP Complete Transaction Demo")
	fmt.Println("===============================")

	// Create configuration
	cfg := config.DefaultConfig()
	cfg.Storage.Backend = "memory"

	// Build server with transaction support
	amqpServer, err := server.NewServerBuilder().
		WithConfig(cfg).
		WithZapLogger("info").
		Build()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	fmt.Printf("✅ Server created with transaction support\n")
	fmt.Printf("   Storage Backend: %s\n", cfg.Storage.Backend)
	
	// Set up exchanges and queues for the demo
	err = setupBrokerResources(amqpServer)
	if err != nil {
		log.Fatalf("Failed to setup broker resources: %v", err)
	}
	
	// Demonstrate transaction manager
	if amqpServer.TransactionManager != nil {
		fmt.Println("✅ Transaction Manager initialized")
		
		// Get initial statistics
		stats := amqpServer.TransactionManager.GetTransactionStats()
		fmt.Printf("   Active Transactions: %d\n", stats.ActiveTransactions)
		fmt.Printf("   Total Commits: %d\n", stats.TotalCommits)
		fmt.Printf("   Total Rollbacks: %d\n", stats.TotalRollbacks)
		
		// Demo transaction operations with working broker
		demoCompleteTransactionFlow(amqpServer)
	} else {
		fmt.Println("❌ Transaction Manager not available")
	}

	fmt.Println("\nDemo completed successfully!")
}

func setupBrokerResources(server *server.Server) error {
	fmt.Println("\n🔧 Setting up broker resources")
	
	// Declare exchange
	err := server.Broker.DeclareExchange("demo.exchange", "direct", true, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %v", err)
	}
	fmt.Printf("   ✅ Created exchange 'demo.exchange'\n")
	
	// Declare queue
	queue, err := server.Broker.DeclareQueue("demo.queue", true, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %v", err)
	}
	fmt.Printf("   ✅ Created queue '%s'\n", queue.Name)
	
	// Bind queue to exchange
	err = server.Broker.BindQueue("demo.queue", "demo.exchange", "demo.key", nil)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %v", err)
	}
	fmt.Printf("   ✅ Bound queue to exchange with routing key 'demo.key'\n")
	
	return nil
}

func demoCompleteTransactionFlow(server *server.Server) {
	fmt.Println("\n🔄 Complete Transaction Flow Demo")
	
	tm := server.TransactionManager
	channelID := uint16(1)
	
	// Start transaction
	err := tm.Select(channelID)
	if err != nil {
		log.Printf("Failed to select transaction: %v", err)
		return
	}
	fmt.Printf("   ✅ Started transaction on channel %d\n", channelID)
	
	// Check transaction state
	if tm.IsTransactional(channelID) {
		fmt.Printf("   ✅ Channel %d is in transactional mode\n", channelID)
		state := tm.GetState(channelID)
		fmt.Printf("   Transaction state: %v\n", state)
	}
	
	// Create a message
	message := &protocol.Message{
		Exchange:     "demo.exchange",
		RoutingKey:   "demo.key",
		Body:         []byte("Transaction demo message - this will be committed!"),
		DeliveryMode: 2, // Persistent message
	}
	
	// Add publish operation
	publishOp := transaction.NewPublishOperation("demo.exchange", "demo.key", message)
	err = tm.AddOperation(channelID, publishOp)
	if err != nil {
		log.Printf("Failed to add publish operation: %v", err)
		return
	}
	fmt.Printf("   ✅ Added publish operation\n")
	
	// Check pending operations
	pending, err := tm.GetPendingOperations(channelID)
	if err != nil {
		log.Printf("Failed to get pending operations: %v", err)
		return
	}
	fmt.Printf("   📋 Pending operations: %d\n", len(pending))
	
	// Commit transaction - this should now work!
	err = tm.Commit(channelID)
	if err != nil {
		fmt.Printf("   ❌ Commit failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Successfully committed transaction!\n")
		fmt.Printf("   📨 Message published to exchange 'demo.exchange' with routing key 'demo.key'\n")
	}
	
	// Check that operations were cleared after commit
	pending, err = tm.GetPendingOperations(channelID)
	if err != nil {
		log.Printf("Failed to get pending operations: %v", err)
		return
	}
	fmt.Printf("   📋 Pending operations after commit: %d\n", len(pending))
	
	// Demo rollback with new operations
	fmt.Println("\n   🔄 Testing rollback functionality")
	message2 := &protocol.Message{
		Exchange:     "demo.exchange", 
		RoutingKey:   "demo.key",
		Body:         []byte("This message will be rolled back"),
		DeliveryMode: 2,
	}
	
	publishOp2 := transaction.NewPublishOperation("demo.exchange", "demo.key", message2)
	err = tm.AddOperation(channelID, publishOp2)
	if err != nil {
		log.Printf("Failed to add operation: %v", err)
		return
	}
	fmt.Printf("   ✅ Added operation for rollback test\n")
	
	// Rollback
	err = tm.Rollback(channelID)
	if err != nil {
		log.Printf("Failed to rollback: %v", err)
		return
	}
	fmt.Printf("   ✅ Successfully rolled back transaction (message discarded)\n")
	
	// Final statistics
	stats := tm.GetTransactionStats()
	fmt.Printf("\n   📊 Final Statistics:\n")
	fmt.Printf("      Active Transactions: %d\n", stats.ActiveTransactions)
	fmt.Printf("      Total Commits: %d\n", stats.TotalCommits)
	fmt.Printf("      Total Rollbacks: %d\n", stats.TotalRollbacks)
	fmt.Printf("      Operation Counts: %v\n", stats.OperationCounts)
	
	// Clean up
	err = tm.Close(channelID)
	if err != nil {
		log.Printf("Failed to close transaction: %v", err)
		return
	}
	fmt.Printf("   ✅ Closed transaction on channel %d\n", channelID)
}