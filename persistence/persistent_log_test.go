package persistence

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func createTempFile(t testing.TB, initialData string) (io.ReadWriteSeeker, func()) {
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

	log := PersistentKVLog{database}

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

	log := PersistentKVLog{database}

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

	log := PersistentKVLog{database}

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

	log := PersistentKVLog{database}

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
