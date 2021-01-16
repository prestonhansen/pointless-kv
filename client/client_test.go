package client

import "testing"

func TestPutAndGet(t *testing.T) {
	key := "key"
	value := "value"

	Put(key, value)

	got := Get(key)
	want := value

	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
