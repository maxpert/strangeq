//go:build !darwin

package clusterraft

import "os"

func syncJournalFile(file *os.File) error { return file.Sync() }
