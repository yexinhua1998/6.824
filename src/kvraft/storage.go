package kvraft

import (
	"fmt"
	"sync"
)

type KvStorage struct {
	storage map[string]string
	rwLock  sync.RWMutex
}

func NewKvStorage() *KvStorage {
	return &KvStorage{
		storage: make(map[string]string),
	}
}

func (s *KvStorage) Get(key string) string {
	s.rwLock.RLock()
	value := s.storage[key]
	fmt.Printf("storage: key=%s value=%s storage=%+v\n", key, value, s.storage)
	s.rwLock.RUnlock()
	return value
}

func (s *KvStorage) Put(key, value string) {
	s.rwLock.Lock()
	s.storage[key] = value
	s.rwLock.Unlock()
}

func (s *KvStorage) Append(key, value string) {
	s.rwLock.Lock()
	raw := s.storage[key]
	s.storage[key] = raw + value
	s.rwLock.Unlock()
}
