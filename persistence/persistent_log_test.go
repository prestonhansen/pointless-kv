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

// Covers the case where we don't have a key indexed. Should figure out
// how to test that behavior more directly and less through assumed implementation
// details as in here.
func TestGetSameKeyMultipleTimes(t *testing.T) {
	database, cleanDatabase := createTempFile(t, "key1,value1\n")
	defer cleanDatabase()

	log := NewPersistentKVLog(database)
	log.GetLatest("key1")
	got, err := log.GetLatest("key1")
	want := "value1"
	if err != nil {
		t.Error(err)
	}
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
}

// Covers writing a key (and indexing it) then reading. As above, should cover this
// more directly via refactor.
func TestPutThenGet(t *testing.T) {
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
}
