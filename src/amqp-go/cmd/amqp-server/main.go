package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/server"
	"go.uber.org/zap"
)

const banner = `
    ___   __  ______  ____        ______      
   /   | /  |/  / __ \/ __ \      / ____/___   
  / /| |/ /|_/ / / / / /_/ /_____/ / __/ __ \  
 / ___ / /  / / /_/ / ____/_____/ /_/ / /_/ /  
/_/  |_/_/  /_/\___\_/          \____/\____/   
                                               
AMQP 0.9.1 Server - High Performance Message Broker
Version: %s
`

// Build identity. These are variables, not constants, so -X can stamp them:
//
//	go build -ldflags "-X main.version=0.1.0 -X main.commit=abc1234 -X main.date=..."
//
// An unstamped source build says 0.1.0-dev so it is never mistaken for a
// release. Releases stamp from the tag with the leading "v" stripped.
var (
	version = "0.1.0-dev"
	commit  = "unknown"
	date    = "unknown"
)

// versionString is the one-line --version output.
func versionString() string {
	return fmt.Sprintf("amqp-server %s (commit %s, built %s, %s)", version, commit, date, runtime.Version())
}

// cliOptions holds the parsed flag values. The pointers exist so the flag
// package owns storage; applyFlagOverrides only acts on flags the operator
// actually set (flag.Visit), so defaults never clobber config values.
type cliOptions struct {
	configFile       *string
	showVersion      *bool
	generateConfig   *string
	enableTelemetry  *bool
	telemetryPort    *int
	telemetryAddress *string
	enablePprof      *bool
	healthcheck      *bool
	tlsEnable        *bool
	tlsCert          *string
	tlsKey           *string
	tlsCA            *string
	unsafeRecovery   *bool
}

// newFlagSet defines the final CLI surface. Flag defaults are documentation
// only (0/""/false) because config supplies the real defaults; applyFlagOverrides
// uses flag.Visit so an unset flag never overwrites the config.
func newFlagSet() (*flag.FlagSet, *cliOptions) {
	fs := flag.NewFlagSet("amqp-server", flag.ExitOnError)
	o := &cliOptions{}

	o.configFile = fs.String("config", "", "Configuration file path (YAML/JSON)")
	o.showVersion = fs.Bool("version", false, "Show version and exit")
	o.generateConfig = fs.String("generate-config", "", "Generate default config file (mode 0600) and exit (e.g., config.yaml)")
	o.enableTelemetry = fs.Bool("enable-telemetry", false, "Enable telemetry HTTP endpoint (/metrics, /health)")
	o.telemetryPort = fs.Int("telemetry-port", 0, "Telemetry HTTP server port (default from config: 9419)")
	o.telemetryAddress = fs.String("telemetry-address", "", "Telemetry bind address (default from config: 127.0.0.1)")
	o.enablePprof = fs.Bool("enable-pprof", false, "Enable pprof profiling endpoints (off by default; loopback only)")
	o.healthcheck = fs.Bool("healthcheck", false, "Probe the local /health endpoint and exit 0 (ready) or 1 (not ready)")

	o.tlsEnable = fs.Bool("tls", false, "Enable TLS (amqps)")
	o.tlsCert = fs.String("tls-cert", "", "Path to TLS certificate file (PEM)")
	o.tlsKey = fs.String("tls-key", "", "Path to TLS private key file (PEM)")
	o.tlsCA = fs.String("tls-ca", "", "Path to TLS CA file for mutual TLS client verification (PEM)")

	o.unsafeRecovery = fs.Bool("unsafe-recovery", false,
		"DATA-LOSS SWITCH. Start even when recovery cannot be completed safely, DISCARDING every artifact the broker could not recover. "+
			"Each discarded artifact is logged by path at ERROR level, the amqp_unsafe_recovery_discarded_artifacts gauge is pinned above zero "+
			"for the lifetime of the process, and the broker re-warns every 5 minutes. Confirmed durable messages in the discarded artifacts are "+
			"NOT recovered. Prefer restoring from a backup, or moving the named files aside so you still have them, before using this.")

	return fs, o
}

// applyFlagOverrides applies only explicitly-set flags onto cfg.
func applyFlagOverrides(cfg *config.AMQPConfig, fs *flag.FlagSet) error {
	var firstErr error
	fs.Visit(func(f *flag.Flag) {
		value := f.Value.String()
		switch f.Name {
		case "tls":
			cfg.Security.TLSEnabled = value == "true"
		case "tls-cert":
			cfg.Security.TLSCertFile = value
		case "tls-key":
			cfg.Security.TLSKeyFile = value
		case "tls-ca":
			cfg.Security.TLSCAFile = value
		case "unsafe-recovery":
			cfg.Storage.UnsafeRecovery = value == "true"
		case "enable-telemetry":
			cfg.Telemetry.Enabled = value == "true"
		case "telemetry-port":
			port, err := strconv.Atoi(value)
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("invalid --telemetry-port %q: %w", value, err)
				}
				return
			}
			cfg.Telemetry.Port = port
		case "telemetry-address":
			cfg.Telemetry.Address = value
		case "enable-pprof":
			cfg.Telemetry.PprofEnabled = value == "true"
		}
	})
	return firstErr
}

func printLoadReport(w io.Writer, report *config.LoadReport) {
	config.PrintLoadReport(w, report)
}

func main() {
	fs, opts := newFlagSet()
	_ = fs.Parse(os.Args[1:])

	if *opts.showVersion {
		fmt.Println(versionString())
		return
	}

	if *opts.generateConfig != "" {
		cfg := config.DefaultConfig()
		if err := cfg.Save(*opts.generateConfig); err != nil {
			log.Fatalf("Failed to generate config file: %v", err)
		}
		fmt.Printf("Generated default configuration: %s\n", *opts.generateConfig)
		fmt.Println("Edit the file and start server with: amqp-server --config " + *opts.generateConfig)
		return
	}

	if *opts.healthcheck {
		os.Exit(runHealthcheck(*opts.configFile))
	}

	if os.Getenv(daemonEnv) == "" {
		fmt.Printf(banner, version)
	}

	// Load: defaults -> file -> env, then explicitly-set flags. The --config
	// path is made absolute before loading so a later daemon chdir cannot
	// change which file was meant.
	configPath := *opts.configFile
	if configPath != "" {
		abs, err := filepath.Abs(configPath)
		if err != nil {
			log.Fatalf("Failed to resolve config file path %s: %v", configPath, err)
		}
		configPath = abs
	}

	cfg, report, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	if err := applyFlagOverrides(cfg, fs); err != nil {
		log.Fatalf("Invalid flags: %v", err)
	}

	cfg.Server.Version = version
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to determine working directory: %v", err)
	}
	cfg.ResolvePaths(cwd)
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	printLoadReport(os.Stderr, report)
	emitSecurityWarnings(os.Stderr, nil, cfg.SecurityWarnings())
	for _, mechanism := range cfg.UnknownMechanisms() {
		fmt.Fprintf(os.Stderr, "unknown auth mechanism %q ignored (supported: PLAIN, ANONYMOUS)\n", mechanism)
	}
	if cfg.Telemetry.PprofEnabled && !cfg.Telemetry.Enabled {
		fmt.Fprintln(os.Stderr, "warning: --enable-pprof has no effect without --enable-telemetry")
	}
	if cfg.Storage.UnsafeRecovery {
		fmt.Println("*** --unsafe-recovery IS SET: this broker may have discarded unrecoverable data at boot. See the log lines above. ***")
	}

	// Daemon stages. Stage 0 waits for READY; stage 1 detaches and forks; the
	// final process resolves paths (above) before finalizeDaemon chdirs.
	switch os.Getenv(daemonEnv) {
	case "":
		if cfg.Server.Daemonize {
			if err := startDaemon(cfg.Server.LogFile); err != nil {
				log.Fatalf("Failed to daemonize: %v", err)
			}
			os.Exit(0)
		}
	case "1":
		if err := forkFinalDaemon(); err != nil {
			log.Fatalf("Failed to daemonize: %v", err)
		}
		os.Exit(0)
	case "2":
		if err := finalizeDaemon(cfg.Server.LogFile); err != nil {
			log.Fatalf("Failed to finalize daemon: %v", err)
		}
	}
	readyFile := daemonReadyFile()

	// Collector must exist before Build (the server binds it for hot-path
	// counters). The HTTP server is constructed only after Build returns, so
	// /health cannot report ready while recovery is still replaying the WAL.
	var collector *metrics.Collector
	if cfg.Telemetry.Enabled {
		collector = metrics.NewCollector("amqp")
		collector.SetBuildInfo(version, commit, runtime.Version())
	}

	serverBuilder := server.NewServerBuilder().WithConfig(cfg)
	if collector != nil {
		serverBuilder = serverBuilder.WithMetrics(collector)
	}
	amqpServer, err := serverBuilder.Build()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Warnings again through the logger so they land in the journal/log file.
	for _, warning := range cfg.SecurityWarnings() {
		amqpServer.Log.Warn(warning, zap.String("component", "startup-security"))
	}
	for _, key := range report.UnknownKeys {
		amqpServer.Log.Warn(fmt.Sprintf("unknown config key %q ignored (check spelling; see config.sample.yaml)", key),
			zap.String("component", "startup-config"))
	}

	if cfg.Server.PidFile != "" {
		if err := writePIDFile(cfg.Server.PidFile); err != nil {
			log.Fatalf("Failed to write PID file: %v", err)
		}
	}

	var shuttingDown atomic.Bool
	var telemetryServer *metrics.Server
	var stopPoller context.CancelFunc
	if cfg.Telemetry.Enabled {
		telemetryServer = metrics.NewServerWithOptions(metrics.ServerOptions{
			Address:     cfg.Telemetry.Address,
			Port:        cfg.Telemetry.Port,
			EnablePprof: cfg.Telemetry.PprofEnabled,
			Ready:       readyFunc(amqpServer.IsListening, serverHealth(amqpServer), &shuttingDown),
			Annotate:    func() string { return alarmAnnotation(amqpServer.AlarmState()) },
		})
		if err := telemetryServer.Listen(); err != nil {
			log.Fatalf("telemetry: %v", err)
		}
		go func() {
			if err := telemetryServer.Serve(); err != nil {
				log.Printf("telemetry server failed: %v", err)
			}
		}()

		pollerCtx, cancel := context.WithCancel(context.Background())
		stopPoller = cancel
		go collector.RunPoller(pollerCtx, metrics.PollerOptions{
			Interval: 15 * time.Second,
			Queues:   brokerQueueStats{b: amqpServer.Broker},
			Alarms:   amqpServer.AlarmState,
			Log:      amqpServer.Log,
		})

		base := "http://" + telemetryServer.Addr().String()
		fmt.Printf("Telemetry server listening on %s\n", base)
		fmt.Printf("  Prometheus metrics: %s/metrics\n", base)
		fmt.Printf("  Health check: %s/health\n", base)
		if cfg.Telemetry.PprofEnabled {
			fmt.Printf("  Profiling index: %s/debug/pprof/\n", base)
			fmt.Printf("  CPU profile: curl -o cpu.prof '%s/debug/pprof/profile?seconds=30'\n", base)
		}
	}

	// Signal handling must be installed before Start: SIGHUP's default action
	// terminates the process.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	go func() {
		for range hupCh {
			reloadAuth(amqpServer)
		}
	}()

	fmt.Printf("Starting AMQP server on %s\n", cfg.Network.Address)
	fmt.Printf("Storage path: %s (persistent)\n", cfg.Storage.Path)
	if cfg.Security.TLSEnabled {
		if cfg.Security.TLSCAFile != "" {
			fmt.Println("TLS: Enabled (mutual TLS, client cert required)")
		} else {
			fmt.Println("TLS: Enabled")
		}
	}
	if cfg.Security.AuthenticationEnabled {
		fmt.Printf("Authentication: Enabled (%s)\n", cfg.Security.AuthenticationBackend)
	}
	fmt.Printf("Log level: %s\n", cfg.Server.LogLevel)

	errCh := make(chan error, 1)
	go func() { errCh <- amqpServer.Start() }()

	for !amqpServer.IsListening() {
		select {
		case err := <-errCh:
			log.Fatalf("Server failed: %v", err)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	notifyDaemonReady(readyFile)
	if !isDaemonChild() {
		fmt.Println("Server ready - Press Ctrl+C to stop")
	}

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("Server failed: %v", err)
		}
		os.Exit(0)
	case <-sigCh:
	}

	// Flip readiness first so load balancers drain before the listener closes.
	shuttingDown.Store(true)
	fmt.Println("\nShutting down gracefully (30s budget)...")

	// A second signal means "stop waiting".
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "second signal received: forcing exit")
		os.Exit(130)
	}()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	shutdownErr := shutdownServer(amqpServer, shutdownCtx)
	cancelShutdown()

	if stopPoller != nil {
		stopPoller()
	}
	if telemetryServer != nil {
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		_ = telemetryServer.Stop(stopCtx)
		cancelStop()
	}
	if cfg.Server.PidFile != "" {
		_ = os.Remove(cfg.Server.PidFile)
	}
	_ = amqpServer.Log.Sync()

	if shutdownErr != nil {
		log.Printf("Shutdown did not complete cleanly: %v", shutdownErr)
		os.Exit(1)
	}
	fmt.Println("Server stopped")
	os.Exit(0)
}

// reloadAuth implements SIGHUP: re-read the auth file when the authenticator
// supports it; never exit. Existing connections keep their user snapshot.
func reloadAuth(s *server.Server) {
	if reloader, ok := s.Authenticator.(interface{ Reload() error }); ok {
		if err := reloader.Reload(); err != nil {
			s.Log.Error("failed to reload auth file", zap.Error(err))
			return
		}
		s.Log.Info("auth file reloaded")
		return
	}
	s.Log.Info("SIGHUP received: nothing to reload")
}

// runHealthcheck is the container probe mode. It reads the same config (and
// env) as the server, probes loopback when the bind is unspecified, and exits
// 0 only on HTTP 200.
func runHealthcheck(configPath string) int {
	if configPath != "" {
		if abs, err := filepath.Abs(configPath); err == nil {
			configPath = abs
		}
	}
	cfg, _, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: failed to load configuration: %v\n", err)
		return 1
	}
	url := healthcheckURL(cfg.Telemetry.Address, cfg.Telemetry.Port)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: %s returned %d\n", url, resp.StatusCode)
		return 1
	}
	return 0
}

func writePIDFile(pidFile string) error {
	pid := os.Getpid()
	return os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", pid)), 0644)
}
