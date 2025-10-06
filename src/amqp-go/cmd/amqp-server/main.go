package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

const (
	version = "0.9.1"
	banner  = `
    ___   __  ______  ____        ______      
   /   | /  |/  / __ \/ __ \      / ____/___   
  / /| |/ /|_/ / / / / /_/ /_____/ / __/ __ \  
 / ___ / /  / / /_/ / ____/_____/ /_/ / /_/ /  
/_/  |_/_/  /_/\___\_/          \____/\____/   
                                              
AMQP 0.9.1 Server - High Performance Message Broker
Version: %s
`
)

func main() {
	// Define command-line flags
	var (
		// Configuration file
		configFile = flag.String("config", "", "Configuration file path (JSON)")
		showVersion = flag.Bool("version", false, "Show version and exit")
		
		// Network configuration
		addr = flag.String("addr", ":5672", "Server bind address")
		port = flag.Int("port", 5672, "Server port")
		maxConnections = flag.Int("max-connections", 1000, "Maximum concurrent connections")
		
		// Storage configuration
		storageBackend = flag.String("storage", "memory", "Storage backend (memory, badger)")
		storagePath = flag.String("storage-path", "", "Storage directory path (required for persistent backends)")
		syncWrites = flag.Bool("sync-writes", false, "Enable synchronous writes for durability")
		cacheSize = flag.String("cache-size", "64MB", "Storage cache size (e.g., 64MB, 128MB)")
		
		// Security configuration
		tlsEnabled = flag.Bool("tls", false, "Enable TLS")
		tlsCert = flag.String("tls-cert", "", "TLS certificate file")
		tlsKey = flag.String("tls-key", "", "TLS private key file")
		authEnabled = flag.Bool("auth", false, "Enable authentication")
		authFile = flag.String("auth-file", "", "Authentication file path")
		
		// Server configuration
		logLevel = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		logFile = flag.String("log-file", "", "Log file path (empty = stdout)")
		pidFile = flag.String("pid-file", "", "PID file path")
		daemonize = flag.Bool("daemonize", false, "Run as daemon")
		maxChannels = flag.Int("max-channels", 2047, "Maximum channels per connection")
		maxFrameSize = flag.Int("max-frame-size", 131072, "Maximum frame size in bytes")
		maxMessageSize = flag.String("max-message-size", "16MB", "Maximum message size (e.g., 16MB)")
	)

	flag.Parse()

	// Show version and exit
	if *showVersion {
		fmt.Printf("AMQP-Go Server version %s\n", version)
		return
	}

	// Show banner
	fmt.Printf(banner, version)
	
	// Create configuration
	var cfg *config.AMQPConfig
	var err error

	if *configFile != "" {
		// Load from configuration file
		cfg = &config.AMQPConfig{}
		if err := cfg.Load(*configFile); err != nil {
			log.Fatalf("Failed to load configuration file %s: %v", *configFile, err)
		}
		fmt.Printf("Loaded configuration from: %s\n", *configFile)
	} else {
		// Build configuration from command-line flags
		cfg, err = buildConfigFromFlags(
			*addr, *port, *maxConnections,
			*storageBackend, *storagePath, *syncWrites, *cacheSize,
			*tlsEnabled, *tlsCert, *tlsKey, *authEnabled, *authFile,
			*logLevel, *logFile, *pidFile, *daemonize,
			*maxChannels, *maxFrameSize, *maxMessageSize,
		)
		if err != nil {
			log.Fatalf("Failed to build configuration: %v", err)
		}
		fmt.Println("Using configuration from command-line flags")
	}

	// Create and start server
	serverBuilder := server.NewServerBuilder().WithConfig(cfg)
	
	amqpServer, err := serverBuilder.Build()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Write PID file if specified
	if cfg.Server.PidFile != "" {
		if err := writePIDFile(cfg.Server.PidFile); err != nil {
			log.Fatalf("Failed to write PID file: %v", err)
		}
	}

	// Setup signal handling for graceful shutdown
	setupSignalHandling(amqpServer, cfg.Server.PidFile)

	// Start server
	fmt.Printf("Starting AMQP server on %s\n", cfg.Network.Address)
	fmt.Printf("Storage backend: %s\n", cfg.Storage.Backend)
	if cfg.Storage.Persistent {
		fmt.Printf("Storage path: %s\n", cfg.Storage.Path)
	}
	if cfg.Security.TLSEnabled {
		fmt.Println("TLS: Enabled")
	}
	if cfg.Security.AuthenticationEnabled {
		fmt.Printf("Authentication: Enabled (%s)\n", cfg.Security.AuthenticationBackend)
	}
	fmt.Printf("Transaction support: Enabled\n")
	fmt.Printf("Log level: %s\n", cfg.Server.LogLevel)
	fmt.Println("Server ready - Press Ctrl+C to stop")

	if err := amqpServer.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func buildConfigFromFlags(
	addr string, port, maxConnections int,
	storageBackend, storagePath string, syncWrites bool, cacheSize string,
	tlsEnabled bool, tlsCert, tlsKey string, authEnabled bool, authFile string,
	logLevel, logFile, pidFile string, daemonize bool,
	maxChannels, maxFrameSize int, maxMessageSize string,
) (*config.AMQPConfig, error) {
	
	builder := config.NewConfigBuilder().
		// Network configuration
		WithAddress(addr).
		WithPort(port).
		WithMaxConnections(maxConnections).
		
		// Server configuration
		WithLogging(logLevel, logFile).
		WithDaemonize(daemonize, pidFile).
		WithProtocolLimits(maxChannels, maxFrameSize, parseSize(maxMessageSize))

	// Storage configuration
	switch storageBackend {
	case "memory":
		builder = builder.WithMemoryStorage()
	case "badger":
		if storagePath == "" {
			return nil, fmt.Errorf("storage path required for badger backend")
		}
		builder = builder.WithBadgerStorage(storagePath).
			WithSyncWrites(syncWrites).
			WithCacheSize(parseSize(cacheSize))
	default:
		return nil, fmt.Errorf("unsupported storage backend: %s", storageBackend)
	}

	// Security configuration
	if tlsEnabled {
		if tlsCert == "" || tlsKey == "" {
			return nil, fmt.Errorf("TLS certificate and key files required when TLS is enabled")
		}
		builder = builder.WithTLS(tlsCert, tlsKey)
	}

	if authEnabled {
		if authFile == "" {
			return nil, fmt.Errorf("authentication file required when authentication is enabled")
		}
		builder = builder.WithFileAuthentication(authFile)
	}

	return builder.Build()
}

func parseSize(sizeStr string) int64 {
	sizeStr = strings.ToUpper(sizeStr)
	
	var multiplier int64 = 1
	if strings.HasSuffix(sizeStr, "KB") {
		multiplier = 1024
		sizeStr = strings.TrimSuffix(sizeStr, "KB")
	} else if strings.HasSuffix(sizeStr, "MB") {
		multiplier = 1024 * 1024
		sizeStr = strings.TrimSuffix(sizeStr, "MB")
	} else if strings.HasSuffix(sizeStr, "GB") {
		multiplier = 1024 * 1024 * 1024
		sizeStr = strings.TrimSuffix(sizeStr, "GB")
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0
	}

	return size * multiplier
}

func writePIDFile(pidFile string) error {
	pid := os.Getpid()
	return os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", pid)), 0644)
}

func setupSignalHandling(server *server.Server, pidFile string) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		fmt.Println("\nShutting down server gracefully...")
		
		// Clean up PID file
		if pidFile != "" {
			os.Remove(pidFile)
		}

		// Stop server (if server has a Stop method)
		server.Shutdown = true
		if server.Listener != nil {
			server.Listener.Close()
		}

		// Give some time for graceful shutdown
		time.Sleep(2 * time.Second)
		fmt.Println("Server stopped")
		os.Exit(0)
	}()
}