package main

import (
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// TestAuthenticationPLAIN tests PLAIN mechanism authentication
func TestAuthenticationPLAIN(t *testing.T) {
	// Create auth file
	authFile := "/tmp/test_auth_plain.json"
	defer os.Remove(authFile)

	// Start server with PLAIN authentication
	cfg := config.DefaultConfig()
	cfg.Network.Address = ":15672"
	cfg.Security.AuthenticationEnabled = true
	cfg.Security.AuthenticationFilePath = authFile
	cfg.Security.AuthMechanisms = []string{"PLAIN"}

	// Create file authenticator (will auto-create with guest user)
	authenticator, err := auth.NewFileAuthenticator(authFile)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	// Create mechanism registry
	registry := auth.DefaultRegistry()

	// Create server
	srv := server.NewServer(cfg.Network.Address)
	srv.Config = cfg
	srv.Authenticator = authenticator
	srv.MechanismRegistry = server.NewMechanismRegistryAdapter(registry)

	// Start server in background
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Test successful authentication with guest/guest
	t.Run("SuccessfulAuthentication", func(t *testing.T) {
		conn, err := amqp.Dial("amqp://guest:guest@localhost:15672/")
		if err != nil {
			t.Fatalf("Failed to connect with valid credentials: %v", err)
		}
		defer conn.Close()

		// Create channel to verify connection works
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("Failed to create channel: %v", err)
		}
		defer ch.Close()

		t.Log("Successfully authenticated with guest/guest")
	})

	// Test failed authentication with wrong password
	t.Run("FailedAuthentication", func(t *testing.T) {
		conn, err := amqp.Dial("amqp://guest:wrongpass@localhost:15672/")
		if err == nil {
			conn.Close()
			t.Fatal("Expected authentication to fail with wrong password")
		}
		t.Logf("Authentication correctly failed: %v", err)
	})

	// Test failed authentication with unknown user
	t.Run("UnknownUser", func(t *testing.T) {
		conn, err := amqp.Dial("amqp://unknown:password@localhost:15672/")
		if err == nil {
			conn.Close()
			t.Fatal("Expected authentication to fail with unknown user")
		}
		t.Logf("Authentication correctly failed for unknown user: %v", err)
	})

	// Stop server
	srv.Mutex.Lock()
	srv.Shutdown = true
	srv.Mutex.Unlock()
	if srv.Listener != nil {
		srv.Listener.Close()
	}
}

// TestAuthenticationANONYMOUS tests ANONYMOUS mechanism authentication
func TestAuthenticationANONYMOUS(t *testing.T) {
	// Start server with ANONYMOUS authentication
	cfg := config.DefaultConfig()
	cfg.Network.Address = ":15673"
	cfg.Security.AuthenticationEnabled = true
	cfg.Security.AuthMechanisms = []string{"ANONYMOUS"}

	// Create mechanism registry
	registry := auth.DefaultRegistry()

	// Create server
	srv := server.NewServer(cfg.Network.Address)
	srv.Config = cfg
	srv.MechanismRegistry = server.NewMechanismRegistryAdapter(registry)

	// Start server in background
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Test anonymous authentication (any credentials should work)
	t.Run("AnonymousAccess", func(t *testing.T) {
		// Note: AMQP client libraries typically require PLAIN for standard connections
		// ANONYMOUS is advertised but client still uses PLAIN format
		// The ANONYMOUS mechanism in our server accepts any credentials
		conn, err := amqp.Dial("amqp://anything:anything@localhost:15673/")
		if err != nil {
			t.Fatalf("Failed to connect with anonymous authentication: %v", err)
		}
		defer conn.Close()

		// Create channel to verify connection works
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("Failed to create channel: %v", err)
		}
		defer ch.Close()

		t.Log("Successfully connected with anonymous authentication")
	})

	// Stop server
	srv.Mutex.Lock()
	srv.Shutdown = true
	srv.Mutex.Unlock()
	if srv.Listener != nil {
		srv.Listener.Close()
	}
}

// TestAuthenticationDisabled tests that auth can be disabled
func TestAuthenticationDisabled(t *testing.T) {
	// Start server with authentication disabled
	cfg := config.DefaultConfig()
	cfg.Network.Address = ":15674"
	cfg.Security.AuthenticationEnabled = false

	// Create server
	srv := server.NewServer(cfg.Network.Address)
	srv.Config = cfg

	// Start server in background
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Test connection without authentication
	t.Run("NoAuthentication", func(t *testing.T) {
		conn, err := amqp.Dial("amqp://anything:anything@localhost:15674/")
		if err != nil {
			t.Fatalf("Failed to connect with disabled authentication: %v", err)
		}
		defer conn.Close()

		// Create channel to verify connection works
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("Failed to create channel: %v", err)
		}
		defer ch.Close()

		t.Log("Successfully connected with authentication disabled")
	})

	// Stop server
	srv.Mutex.Lock()
	srv.Shutdown = true
	srv.Mutex.Unlock()
	if srv.Listener != nil {
		srv.Listener.Close()
	}
}

// TestAuthenticationWithMessaging tests that authenticated connections can publish/consume
func TestAuthenticationWithMessaging(t *testing.T) {
	// Create auth file
	authFile := "/tmp/test_auth_messaging.json"
	defer os.Remove(authFile)

	// Start server with PLAIN authentication
	cfg := config.DefaultConfig()
	cfg.Network.Address = ":15675"
	cfg.Security.AuthenticationEnabled = true
	cfg.Security.AuthenticationFilePath = authFile
	cfg.Security.AuthMechanisms = []string{"PLAIN"}

	// Create file authenticator
	authenticator, err := auth.NewFileAuthenticator(authFile)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	// Create mechanism registry
	registry := auth.DefaultRegistry()

	// Create server
	srv := server.NewServer(cfg.Network.Address)
	srv.Config = cfg
	srv.Authenticator = authenticator
	srv.MechanismRegistry = server.NewMechanismRegistryAdapter(registry)

	// Start server in background
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Connect with valid credentials
	conn, err := amqp.Dial("amqp://guest:guest@localhost:15675/")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create channel
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to create channel: %v", err)
	}
	defer ch.Close()

	// Declare queue
	queueName := "test_auth_queue"
	_, err = ch.QueueDeclare(
		queueName,
		false, // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Publish message
	testMessage := "authenticated message"
	err = ch.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(testMessage),
		},
	)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Consume message
	msgs, err := ch.Consume(
		queueName,
		"",    // consumer tag
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}

	// Wait for message
	select {
	case msg := <-msgs:
		if string(msg.Body) != testMessage {
			t.Errorf("Expected message '%s', got '%s'", testMessage, string(msg.Body))
		}
		t.Logf("Successfully published and consumed authenticated message: %s", string(msg.Body))
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Stop server
	srv.Mutex.Lock()
	srv.Shutdown = true
	srv.Mutex.Unlock()
	if srv.Listener != nil {
		srv.Listener.Close()
	}
}
