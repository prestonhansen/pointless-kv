package persistence

type KVLog interface {
	Append(key string, value string)
	GetLatest(key string)
}
