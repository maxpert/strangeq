package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/server"
	"golang.org/x/sys/unix"
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
		configFile      = flag.String("config", "", "Configuration file path (YAML/JSON)")
		showVersion     = flag.Bool("version", false, "Show version and exit")
		generateConfig  = flag.String("generate-config", "", "Generate default config file and exit (e.g., config.yaml)")
		enableTelemetry = flag.Bool("enable-telemetry", false, "Enable telemetry endpoint (Prometheus + pprof profiling)")
		telemetryPort   = flag.Int("telemetry-port", 9419, "Telemetry HTTP server port")
	)

	flag.Parse()

	// Show version and exit
	if *showVersion {
		fmt.Printf("AMQP-Go Server version %s\n", version)
		return
	}

	// Generate default config and exit
	if *generateConfig != "" {
		cfg := config.DefaultConfig()
		if err := cfg.Save(*generateConfig); err != nil {
			log.Fatalf("Failed to generate config file: %v", err)
		}
		fmt.Printf("Generated default configuration: %s\n", *generateConfig)
		fmt.Println("Edit the file and start server with: amqp-server --config " + *generateConfig)
		return
	}

	// Handle daemon stages first
	if isDaemonChild() {
		// Final daemon process - complete daemonization
		if err := finalizeDaemon(""); err != nil {
			log.Fatalf("Failed to finalize daemon: %v", err)
		}
	}

	// Show banner (only for non-daemon or final daemon process)
	if !isDaemonChild() {
		fmt.Printf(banner, version)
	}

	// Load configuration
	var cfg *config.AMQPConfig
	if *configFile != "" {
		// Load from configuration file
		cfg = &config.AMQPConfig{}
		if err := cfg.Load(*configFile); err != nil {
			log.Fatalf("Failed to load configuration file %s: %v", *configFile, err)
		}
		if !isDaemonChild() {
			fmt.Printf("Loaded configuration from: %s\n", *configFile)
		}
	} else {
		// Use default configuration (can be overridden by AMQP_* environment variables)
		cfg = config.DefaultConfig()
		if !isDaemonChild() {
			fmt.Println("Using default configuration (override with --config or AMQP_* env vars)")
		}
	}

	// Handle daemonization if requested (and not already a daemon)
	if cfg.Server.Daemonize && !isDaemonChild() {
		if err := startDaemon(cfg.Server.LogFile); err != nil {
			log.Fatalf("Failed to daemonize: %v", err)
		}
	}

	// Finalize daemon setup if we're the final daemon process
	if isDaemonChild() {
		if err := finalizeDaemon(cfg.Server.LogFile); err != nil {
			log.Fatalf("Failed to finalize daemon: %v", err)
		}
	}

	// Create metrics collector if telemetry is enabled
	var metricsCollector *metrics.Collector
	if *enableTelemetry {
		metricsCollector = metrics.NewCollector("amqp")
		telemetryServer := metrics.NewServer(*telemetryPort, true)

		go func() {
			if !cfg.Server.Daemonize || isDaemonChild() {
				if isDaemonChild() {
					log.Printf("Telemetry server listening on http://localhost:%d", *telemetryPort)
					log.Printf("  Prometheus metrics: http://localhost:%d/metrics", *telemetryPort)
					log.Printf("  Profiling enabled: http://localhost:%d/debug/pprof/", *telemetryPort)
				} else {
					fmt.Printf("Telemetry server listening on http://localhost:%d\n", *telemetryPort)
					fmt.Printf("  Prometheus metrics: http://localhost:%d/metrics\n", *telemetryPort)
					fmt.Printf("  Health check: http://localhost:%d/health\n", *telemetryPort)
					fmt.Printf("  Profiling index: http://localhost:%d/debug/pprof/\n", *telemetryPort)
					fmt.Printf("  CPU profile: curl -o cpu.prof http://localhost:%d/debug/pprof/profile?seconds=30\n", *telemetryPort)
					fmt.Printf("  Mutex profile: curl -o mutex.prof http://localhost:%d/debug/pprof/mutex\n", *telemetryPort)
				}
			}
			if err := telemetryServer.Start(); err != nil {
				log.Printf("Telemetry server failed: %v", err)
			}
		}()
	}

	// Create and start server
	serverBuilder := server.NewServerBuilder().WithConfig(cfg)

	// Add metrics collector if telemetry is enabled
	if metricsCollector != nil {
		serverBuilder = serverBuilder.WithMetrics(metricsCollector)
	}

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

	// Start server - show startup info only if not daemonizing
	if !cfg.Server.Daemonize || isDaemonChild() {
		if isDaemonChild() {
			log.Printf("Starting AMQP server daemon on %s", cfg.Network.Address)
			log.Printf("Storage path: %s (persistent)", cfg.Storage.Path)
			if cfg.Security.TLSEnabled {
				log.Println("TLS: Enabled")
			}
			if cfg.Security.AuthenticationEnabled {
				log.Printf("Authentication: Enabled (%s)", cfg.Security.AuthenticationBackend)
			}
			log.Printf("Transaction support: Enabled")
			log.Printf("Log level: %s", cfg.Server.LogLevel)
			log.Println("AMQP daemon started successfully")
		} else {
			fmt.Printf("Starting AMQP server on %s\n", cfg.Network.Address)
			fmt.Printf("Storage path: %s (persistent)\n", cfg.Storage.Path)
			if cfg.Security.TLSEnabled {
				fmt.Println("TLS: Enabled")
			}
			if cfg.Security.AuthenticationEnabled {
				fmt.Printf("Authentication: Enabled (%s)\n", cfg.Security.AuthenticationBackend)
			}
			fmt.Printf("Transaction support: Enabled\n")
			fmt.Printf("Log level: %s\n", cfg.Server.LogLevel)
			fmt.Println("Server ready - Press Ctrl+C to stop")
		}
	}

	if err := amqpServer.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
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

// startDaemon turns the process into a proper Unix daemon using double-fork technique
func startDaemon(logFile string) error {
	// Check if we're already a daemon (child process)
	if os.Getenv("_AMQP_DAEMON") == "1" {
		// We're the final daemon process - just do final setup
		return setupDaemonEnvironment(logFile)
	}

	// First fork - create child process
	args := make([]string, len(os.Args))
	copy(args, os.Args)

	cmd := &exec.Cmd{
		Path: os.Args[0],
		Args: args,
		Env:  append(os.Environ(), "_AMQP_DAEMON=1"),
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start daemon process: %v", err)
	}

	// Parent exits immediately
	fmt.Printf("AMQP server daemonized with PID %d\n", cmd.Process.Pid)
	os.Exit(0)
	return nil // Never reached
}

// setupDaemonEnvironment sets up the daemon environment for the child process
func setupDaemonEnvironment(logFile string) error {
	// Create new session
	if _, err := unix.Setsid(); err != nil {
		return fmt.Errorf("setsid failed: %v", err)
	}

	// Second fork - prevents daemon from ever acquiring controlling terminal
	cmd := &exec.Cmd{
		Path: os.Args[0],
		Args: os.Args,
		Env:  append(os.Environ(), "_AMQP_DAEMON=2"),
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("second fork failed: %v", err)
	}

	// First child exits
	os.Exit(0)
	return nil // Never reached
}

// isDaemonChild checks if this is the final daemon process
func isDaemonChild() bool {
	return os.Getenv("_AMQP_DAEMON") == "2"
}

// finalizeDaemon completes the daemonization process
func finalizeDaemon(logFile string) error {
	// Change working directory to root
	if err := os.Chdir("/"); err != nil {
		return fmt.Errorf("failed to change directory to /: %v", err)
	}

	// Set file creation mask
	unix.Umask(0)

	// Redirect standard file descriptors
	if err := redirectStdFiles(logFile); err != nil {
		return fmt.Errorf("failed to redirect standard files: %v", err)
	}

	return nil
}

// redirectStdFiles redirects stdin to /dev/null and stdout/stderr to log file or /dev/null
func redirectStdFiles(logFile string) error {
	// Redirect stdin to /dev/null
	devNull, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to open /dev/null: %v", err)
	}

	// Duplicate stdin to /dev/null
	if err := unix.Dup2(int(devNull.Fd()), int(os.Stdin.Fd())); err != nil {
		devNull.Close()
		return fmt.Errorf("failed to redirect stdin: %v", err)
	}

	// Handle stdout and stderr
	var outputFile *os.File
	if logFile != "" {
		// Redirect to log file
		outputFile, err = os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			devNull.Close()
			return fmt.Errorf("failed to open log file %s: %v", logFile, err)
		}
	} else {
		// Redirect to /dev/null if no log file specified
		outputFile = devNull
	}

	// Redirect stdout and stderr
	if err := unix.Dup2(int(outputFile.Fd()), int(os.Stdout.Fd())); err != nil {
		outputFile.Close()
		devNull.Close()
		return fmt.Errorf("failed to redirect stdout: %v", err)
	}

	if err := unix.Dup2(int(outputFile.Fd()), int(os.Stderr.Fd())); err != nil {
		outputFile.Close()
		devNull.Close()
		return fmt.Errorf("failed to redirect stderr: %v", err)
	}

	// Don't close the files as they're now being used by the standard descriptors
	return nil
}
