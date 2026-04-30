package store

import "sync"

type KV struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *KV {
	return &KV{
		data: make(map[string]string),
	}
}

func (kv *KV) Put(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

func (kv *KV) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[key]
	return val, ok
}