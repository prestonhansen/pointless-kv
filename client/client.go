package client

// replace this with persistent storage
var dictionary map[string]string = make(map[string]string)

func Get(key string) string {
	return dictionary[key]
}

func Put(key string, value string) error {
	dictionary[key] = value
	return nil
}
