package ymsql

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v2"
)

type Store interface {
	MergeVariables(vals map[string]string) map[string]string
	SETEnv(name string, value string)
	Load(name string) (Scripting, error)
	Store(bytes []byte) error
	StoreFromFile(path string) error
	StoreFromDirectory(path string) error
	StoreResources(res map[string][]byte) error
}

type YMLStore struct {
	env     sync.Map
	scripts sync.Map
}

func NewYMLStore() Store {
	return &YMLStore{}
}

func (store *YMLStore) MergeVariables(vals map[string]string) map[string]string {
	result := make(map[string]string)
	store.env.Range(func(k interface{}, v interface{}) bool {
		result[k.(string)] = v.(string)
		return true
	})
	for k, v := range vals {
		result[k] = v
	}
	return result
}

func (store *YMLStore) SETEnv(name string, value string) {
	store.env.Store(name, value)
}

func (store *YMLStore) Store(bytes []byte) error {
	var m YMLModel
	err := yaml.Unmarshal(bytes, &m)
	if err != nil {
		return err
	}
	if m.Name == "" {
		return fmt.Errorf("section '%s' required", "name")
	}
	if m.Script == "" {
		return fmt.Errorf("section '%s' required", "script")
	}
	if m.Variables == nil {
		m.Variables = make(map[string]string)
	}

	store.scripts.Store(m.Name, &m)
	return nil
}

func (store *YMLStore) Load(name string) (Scripting, error) {
	s, ok := store.scripts.Load(name)
	if ok == false {
		return nil, fmt.Errorf("script %s not found", name)
	}

	script := &YMLScripting{
		s:         s.(*YMLModel),
		store:     store,
		variables: store.MergeVariables(s.(*YMLModel).Variables),
	}

	return script, nil
}

func (store *YMLStore) StoreFromFile(path string) error {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = store.Store(file)
	if err != nil {
		return err
	}
	return nil
}

func (store *YMLStore) StoreFromDirectory(path string) error {
	dirList, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for _, file := range dirList {
		if file.IsDir() == false && strings.HasSuffix(file.Name(), ".yml") {
			err = store.StoreFromFile(filepath.Join(path, file.Name()))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (store *YMLStore) StoreResources(res map[string][]byte) error {
	for _, v := range res {
		err := store.Store(v)
		if err != nil {
			return err
		}
	}
	return nil
}
