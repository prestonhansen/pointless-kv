package persistence

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type PersistentKVLog struct {
	seeker io.ReadWriteSeeker
}

func (log *PersistentKVLog) GetLatest(key string) (string, error) {
	// go to start of data
	log.seeker.Seek(0, 0)

	// read until we find our key
	scanner := bufio.NewScanner(log.seeker)
	var latestValue string
	for scanner.Scan() {
		line := scanner.Text()
		// todo this doesn't handle commas inside the key/value (use regexp?)
		split := strings.Split(line, ",")
		k := split[0]
		v := split[1]
		if k == key {
			latestValue = v
		}
	}
	// todo handle key not found
	// todo handle errors via if err := scanner.Err(); err != nil {}
	return latestValue, nil
}

func (log *PersistentKVLog) Append(key string, value string) {
	// go to end of file
	log.seeker.Seek(0, io.SeekEnd)
	fmt.Fprintf(log.seeker, "%s,%s\n", key, value)
}
