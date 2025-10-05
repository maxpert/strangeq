package main

import (
	"fmt"
	"log"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"github.com/maxpert/amqp-go/transaction"
)

// TransactionDemo demonstrates the pluggable transaction system
func main() {
	fmt.Println("AMQP Transaction System Demo")
	fmt.Println("============================")

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
	
	// Demonstrate transaction manager
	if amqpServer.TransactionManager != nil {
		fmt.Println("✅ Transaction Manager initialized")
		
		// Get initial statistics
		stats := amqpServer.TransactionManager.GetTransactionStats()
		fmt.Printf("   Active Transactions: %d\n", stats.ActiveTransactions)
		fmt.Printf("   Total Commits: %d\n", stats.TotalCommits)
		fmt.Printf("   Total Rollbacks: %d\n", stats.TotalRollbacks)
		
		// Demo transaction operations
		demoTransactionOperations(amqpServer.TransactionManager)
	} else {
		fmt.Println("❌ Transaction Manager not available")
	}

	fmt.Println("\nDemo completed successfully!")
}

func demoTransactionOperations(tm interfaces.TransactionManager) {
	fmt.Println("\n🔄 Transaction Operations Demo")
	
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
	
	// Add some operations (simulated)
	message := &protocol.Message{
		Exchange:   "demo.exchange",
		RoutingKey: "demo.key",
		Body:       []byte("Transaction demo message"),
		DeliveryMode: 2, // Persistent message
	}
	
	publishOp := transaction.NewPublishOperation("demo.exchange", "demo.key", message)
	ackOp := transaction.NewAckOperation("demo.queue", 123, false)
	
	err = tm.AddOperation(channelID, publishOp)
	if err != nil {
		log.Printf("Failed to add publish operation: %v", err)
		return
	}
	fmt.Printf("   ✅ Added publish operation\n")
	
	err = tm.AddOperation(channelID, ackOp)
	if err != nil {
		log.Printf("Failed to add ack operation: %v", err)
		return
	}
	fmt.Printf("   ✅ Added acknowledgment operation\n")
	
	// Check pending operations
	pending, err := tm.GetPendingOperations(channelID)
	if err != nil {
		log.Printf("Failed to get pending operations: %v", err)
		return
	}
	fmt.Printf("   📋 Pending operations: %d\n", len(pending))
	
	// Demonstrate rollback
	err = tm.Rollback(channelID)
	if err != nil {
		log.Printf("Failed to rollback: %v", err)
		return
	}
	fmt.Printf("   ✅ Rolled back transaction (operations discarded)\n")
	
	// Check that operations were cleared
	pending, err = tm.GetPendingOperations(channelID)
	if err != nil {
		log.Printf("Failed to get pending operations: %v", err)
		return
	}
	fmt.Printf("   📋 Pending operations after rollback: %d\n", len(pending))
	
	// Demo commit with new operations
	publishOp2 := transaction.NewPublishOperation("demo.exchange", "commit.key", message)
	err = tm.AddOperation(channelID, publishOp2)
	if err != nil {
		log.Printf("Failed to add operation: %v", err)
		return
	}
	
	// Note: In a real implementation, we would need a broker executor
	// For this demo, commit will fail gracefully due to no executor
	err = tm.Commit(channelID)
	if err != nil {
		fmt.Printf("   ⚠️  Commit failed (expected - no executor set): %v\n", err)
	} else {
		fmt.Printf("   ✅ Committed transaction\n")
	}
	
	// Final statistics
	stats := tm.GetTransactionStats()
	fmt.Printf("   📊 Final Statistics:\n")
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