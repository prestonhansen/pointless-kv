package persistence

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
)

type PersistentKVLog struct {
	file            *os.File
	keyToByteOffset map[string]int64 // our very simple index for O(1) lookups
}

var commaRegex = regexp.MustCompile(`,`)

func (log *PersistentKVLog) GetLatest(key string) (string, error) {
	offset, present := log.keyToByteOffset[key]
	if present {
		log.file.Seek(offset, io.SeekStart)
	} else {
		log.file.Seek(0, io.SeekStart)
	}

	// read until we find our key
	scanner := bufio.NewScanner(log.file)
	var latestValue string
	for scanner.Scan() {
		line := scanner.Text()
		split := commaRegex.Split(line, 2)
		k := split[0]
		v := split[1]
		if k == key {
			latestValue = v
			log.keyToByteOffset[key] = offset
		}
		offset += (int64)(len(line)) + 1
		// if we used the cached offset, we know this is the latest value.
		// no need to scan further.
		if present {
			break
		}
	}
	// todo handle key not found
	// todo handle errors via if err := scanner.Err(); err != nil {}
	return latestValue, nil
}

func (log *PersistentKVLog) Append(key string, value string) error {
	stat, err := log.file.Stat()
	if err != nil {
		// todo
	}
	log.keyToByteOffset[key] = stat.Size()
	log.file.Seek(0, io.SeekEnd)
	// todo how to handle writing values that contain newlines?
	fmt.Fprintf(log.file, "%s,%s\n", key, value)
	return nil
}

func NewPersistentKVLog(file *os.File) *PersistentKVLog {
	return &PersistentKVLog{file, make(map[string]int64)}
}

// Reindex scans through the whole "database" and updates the in memory index.
// Maybe this method could be exposed indirectly via the Client? Give an
// option to reindex automatically on startup? Not sure
func (log *PersistentKVLog) Reindex() error {
	log.file.Seek(0, io.SeekStart)
	var offset int64 = 0
	log.keyToByteOffset = make(map[string]int64)
	scanner := bufio.NewScanner(log.file)
	for scanner.Scan() {
		line := scanner.Text()
		split := commaRegex.Split(line, 2)
		k := split[0]
		log.keyToByteOffset[k] = offset
		offset += (int64)(len(line)) + 1
	}
	return nil
}

func (log *PersistentKVLog) Compact(newFile *os.File) {
	log.file.Seek(0, io.SeekStart)
	scanner := bufio.NewScanner(log.file)
	log.keyToByteOffset = make(map[string]int64)
	log.file = newFile
	keyToLatestValue := make(map[string]string)
	// todo this method assumes we can fit one entry for each (key, value) in oldFile.
	// this seems ok once we use segment files with a capped size, but for now relies
	// on oldFile not containing too many unique keys
	for scanner.Scan() {
		line := scanner.Text()
		split := commaRegex.Split(line, 2)
		key := split[0]
		value := split[1]
		keyToLatestValue[key] = value
	}
	for key, value := range keyToLatestValue {
		log.Append(key, value)
	}
}
