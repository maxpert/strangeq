package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Define command-line flags
	var (
		addr      = flag.String("addr", ":5672", "Address to listen on")
		logLevel  = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		logFile   = flag.String("log-file", "", "Log file path (optional, logs to stdout if not specified)")
		daemonize = flag.Bool("daemonize", false, "Run as daemon (background process)")
	)

	flag.Parse()

	// Create logger with specified level and output
	logger := createLogger(*logLevel, *logFile)
	defer logger.Sync() // flushes buffer, if any

	// If daemonize flag is set, we could implement actual daemonization
	// For now, we'll just log that daemon mode is requested
	if *daemonize {
		logger.Info("Daemon mode requested", zap.Bool("daemonize", *daemonize))
		// In a real implementation, you would:
		// - Fork the process
		// - Redirect stdin, stdout, stderr to /dev/null or log files
		// - Create a new session
		// - Change working directory
		// - Clear file mode creation mask
	}

	// Create server with logger
	srv := &server.Server{
		Addr:        *addr,
		Connections: make(map[string]*protocol.Connection),
		Log:         logger,
	}

	logger.Info("Starting AMQP server...",
		zap.String("address", *addr),
		zap.Bool("daemonize", *daemonize),
		zap.String("log_level", *logLevel),
		zap.String("log_file", *logFile))

	if err := srv.Start(); err != nil {
		logger.Fatal("Failed to start server", zap.Error(err))
	}
}

func createLogger(level string, filePath string) *zap.Logger {
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zap.DebugLevel
	case "info":
		zapLevel = zap.InfoLevel
	case "warn":
		zapLevel = zap.WarnLevel
	case "error":
		zapLevel = zap.ErrorLevel
	default:
		zapLevel = zap.InfoLevel
	}

	var w zapcore.WriteSyncer
	if filePath != "" {
		// Create a file logger
		f, err := os.Create(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create log file: %v\n", err)
			os.Exit(1)
		}
		w = zapcore.AddSync(f)
	} else {
		// Use stdout
		w = zapcore.AddSync(os.Stdout)
	}

	config := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.EpochTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(config),
		w,
		zapLevel,
	)

	return zap.New(core)
}