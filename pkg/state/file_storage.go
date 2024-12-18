package state

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

type FileStateStore struct {
	dir string
}

func NewFileStateStore(dir string) *FileStateStore {
	return &FileStateStore{dir: dir}
}

func (f *FileStateStore) Save(key string, value []byte) error {
	path := filepath.Join(f.dir, key)
	return ioutil.WriteFile(path, value, 0644)
}

func (f *FileStateStore) Load(key string) ([]byte, error) {
	path := filepath.Join(f.dir, key)
	return ioutil.ReadFile(path)
}
