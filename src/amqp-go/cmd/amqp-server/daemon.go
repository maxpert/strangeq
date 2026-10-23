package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	daemonEnv      = "_AMQP_DAEMON"
	daemonReadyEnv = "_AMQP_DAEMON_READY_FD"
	daemonReadyFD  = 3
)

// isDaemonChild reports whether this is the final (fully daemonized) process.
func isDaemonChild() bool {
	return os.Getenv(daemonEnv) == "2"
}

// startDaemon is stage 0: fork once, hand the child the readiness pipe, then
// block until the final process reports READY (or the pipe closes because it
// failed before listening). No timeout: recovery duration is legitimate.
func startDaemon(logFile string) error {
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("failed to create readiness pipe: %w", err)
	}

	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = append(os.Environ(), daemonEnv+"=1", fmt.Sprintf("%s=%d", daemonReadyEnv, daemonReadyFD))
	cmd.ExtraFiles = []*os.File{writePipe}
	if err := cmd.Start(); err != nil {
		readPipe.Close()
		writePipe.Close()
		return fmt.Errorf("failed to start daemon process: %w", err)
	}
	// The parent's copy of the write end would keep the pipe open forever.
	writePipe.Close()

	if err := waitDaemonReady(readPipe); err != nil {
		if logFile == "" {
			fmt.Fprintf(os.Stderr, "daemon failed to start: %v; stderr was /dev/null — set server.logfile to capture the failure\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "daemon failed to start: %v; see %s\n", err, logFile)
		}
		os.Exit(1)
	}

	fmt.Printf("AMQP server daemonized with PID %d\n", cmd.Process.Pid)
	return nil
}

// forkFinalDaemon is stage 1: create a new session (detach from the
// controlling terminal) and fork the final process, passing the readiness fd
// through. The intermediate process exits immediately.
func forkFinalDaemon() error {
	if _, err := unix.Setsid(); err != nil {
		return fmt.Errorf("setsid failed: %v", err)
	}

	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	env := append(os.Environ(), daemonEnv+"=2")
	var readyFile *os.File
	if os.Getenv(daemonReadyEnv) != "" {
		env = append(env, fmt.Sprintf("%s=%d", daemonReadyEnv, daemonReadyFD))
		readyFile = os.NewFile(daemonReadyFD, "daemon-ready")
		if readyFile != nil {
			cmd.ExtraFiles = []*os.File{readyFile}
		}
	}
	cmd.Env = env

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("second fork failed: %w", err)
	}
	if readyFile != nil {
		// The child has its own duplicate after Start.
		_ = readyFile.Close()
	}
	return nil
}

// daemonReadyFile returns the inherited readiness pipe in the final process.
func daemonReadyFile() *os.File {
	if os.Getenv(daemonReadyEnv) == "" {
		return nil
	}
	return os.NewFile(daemonReadyFD, "daemon-ready")
}

// notifyDaemonReady signals the foreground parent that the listener is bound.
func notifyDaemonReady(f *os.File) {
	if f == nil {
		return
	}
	_, _ = fmt.Fprintln(f, "READY")
	_ = f.Close()
}

// waitDaemonReady implements the readiness-pipe protocol: exactly "READY\n"
// means success; EOF without it (the child died) or any other line fails.
func waitDaemonReady(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "READY" {
			return nil
		}
		return fmt.Errorf("unexpected daemon readiness message %q", line)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("daemon readiness: %w", err)
	}
	return errors.New("daemon exited before signalling readiness")
}

// finalizeDaemon completes daemonization in the final process. Paths were
// already resolved against the inherited cwd, so chdir("/") cannot break
// relative log/auth/storage paths. umask 077 keeps broker data (message
// payloads) private; operators needing group access use systemd's UMask=.
func finalizeDaemon(logFile string) error {
	if err := os.Chdir("/"); err != nil {
		return fmt.Errorf("failed to change directory to /: %v", err)
	}

	unix.Umask(0o077)

	if err := redirectStdFiles(logFile); err != nil {
		return fmt.Errorf("failed to redirect standard files: %v", err)
	}

	return nil
}

// redirectStdFiles redirects stdin to /dev/null and stdout/stderr to the log
// file (mode 0600) or /dev/null.
func redirectStdFiles(logFile string) error {
	devNull, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to open /dev/null: %v", err)
	}

	if err := unix.Dup2(int(devNull.Fd()), int(os.Stdin.Fd())); err != nil {
		devNull.Close()
		return fmt.Errorf("failed to redirect stdin: %v", err)
	}

	var outputFile *os.File
	if logFile != "" {
		outputFile, err = os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			devNull.Close()
			return fmt.Errorf("failed to open log file %s: %v", logFile, err)
		}
	} else {
		outputFile = devNull
	}

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

	return nil
}
