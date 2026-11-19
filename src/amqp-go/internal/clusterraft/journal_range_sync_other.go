//go:build !linux

package clusterraft

import "os"

func wrapRangeSyncJournalFile(file *os.File) journalFile {
	return journalDataFile{file}
}

func writeJournalFrame(file journalFile, data []byte, offset int64, _ bool) (int, bool, error) {
	n, err := file.WriteAt(data, offset)
	return n, false, err
}
