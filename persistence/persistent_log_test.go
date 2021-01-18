package persistence

import (
	"io/ioutil"
	"os"
	"testing"
)

func createTempFile(t testing.TB, initialData string) (*os.File, func()) {
	t.Helper()

	tmpfile, err := ioutil.TempFile("", "db")

	if err != nil {
		t.Fatalf("could not create temp file %v", err)
	}

	tmpfile.Write([]byte(initialData))

	removeFile := func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}

	return tmpfile, removeFile
}

func TestGetLatest(t *testing.T) {
	database, cleanDatabase := createTempFile(t, `key1,value1
key2,value2
key1,value3
`)
	defer cleanDatabase()

	log := NewPersistentKVLog(database)

	got, err := log.GetLatest("key1")
	want := "value3"
	if err != nil {
		t.Error(err)
	}
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
}

func TestValueContainsCommas(t *testing.T) {
	database, cleanDatabase := createTempFile(t, `key1,my value, has, commas,
`)
	defer cleanDatabase()

	log := NewPersistentKVLog(database)

	got, err := log.GetLatest("key1")
	want := "my value, has, commas,"
	if err != nil {
		t.Error(err)
	}
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
}

func TestAppendToExistingLog(t *testing.T) {
	database, cleanDatabase := createTempFile(t, `key1,value1
key2,value2
key1,value3
`)
	defer cleanDatabase()

	log := NewPersistentKVLog(database)

	log.Append("key1", "value4")
	got, err := log.GetLatest("key1")
	want := "value4"
	if err != nil {
		t.Error(err)
	}
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
}

func TestAppendToEmptyLog(t *testing.T) {
	database, cleanDatabase := createTempFile(t, "")
	defer cleanDatabase()

	log := NewPersistentKVLog(database)

	log.Append("key1", "value1")
	log.Append("key2", "value2")
	log.Append("key1", "value3")
	got, err := log.GetLatest("key1")
	want := "value3"
	if err != nil {
		t.Error(err)
	}
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
}

// the following tests verify indexing via inspection of the underlying map directly.
// maybe this is a little gross? not sure further abstraction is worth it
func TestGetKeyUpdatesIndex(t *testing.T) {
	database, cleanDatabase := createTempFile(t, "key1,value1\n")
	defer cleanDatabase()

	log := NewPersistentKVLog(database)
	_, indexed := log.keyToByteOffset["key1"]
	if indexed {
		t.Error("Didn't expect key to be indexed on initialization")
	}
	log.GetLatest("key1")
	got, err := log.GetLatest("key1")
	want := "value1"
	if err != nil {
		t.Error(err)
	}
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
	_, indexed = log.keyToByteOffset["key1"]
	if !indexed {
		t.Error("Expected key to be indexed but it wasn't")
	}
}

func TestPutThenGetUpdatesIndex(t *testing.T) {
	database, cleanDatabase := createTempFile(t, "")
	defer cleanDatabase()

	log := NewPersistentKVLog(database)
	log.Append("key1", "value1")
	got, err := log.GetLatest("key1")
	want := "value1"
	if err != nil {
		t.Error(err)
	}
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
	_, indexed := log.keyToByteOffset["key1"]
	if !indexed {
		t.Error("Expected key to be indexed but it wasn't")
	}
}

func TestReindex(t *testing.T) {
	database, cleanDatabase := createTempFile(t, `key1,value1
key2,value2
key1,value3
key3,value4
key3,value5
`)
	defer cleanDatabase()

	log := NewPersistentKVLog(database)
	log.Reindex()
	for _, key := range []string{"key1", "key2", "key3"} {
		_, indexed := log.keyToByteOffset[key]
		if !indexed {
			t.Errorf("Expected %s to be indexed but it wasn't", key)
		}
	}
}
