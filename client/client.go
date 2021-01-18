package client

import (
	"github.com/prestonhansen/pointless-kv/persistence"
	"os"
)

type Client struct {
	log persistence.KVLog
}

func (client *Client) Get(key string) string {
	value, err := client.log.GetLatest(key)
	if err != nil {
		//todo
	}
	return value
}

func (client *Client) Put(key string, value string) error {
	return client.log.Append(key, value)
}

func NewClient(f *os.File) *Client {
	log := persistence.NewPersistentKVLog(f)
	return &Client{log}
}
