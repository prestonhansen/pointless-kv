package persistence

type KVLog interface {
	Append(key string, value string) error
	GetLatest(key string) (string, error)
}
