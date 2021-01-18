package main

import (
	"fmt"
	"github.com/prestonhansen/pointless-kv/client"
	"io/ioutil"
	"os"
)

func main() {
	// todo make this simpler by adding a constructor that takes a File
	tmpfile, err := ioutil.TempFile("", "pointless-db")

	if err != nil {
		fmt.Errorf("Couldn't create temp file")
	}

	removeFile := func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}
	defer removeFile()

	client := client.NewClient(tmpfile)
	client.Put("key", "value")
	fmt.Printf("got %s for key\n", client.Get("key"))
	client.Put("key", "value2")
	fmt.Printf("updated key to %s\n", "value2")
	fmt.Printf("got %s for key\n", client.Get("key"))
}
