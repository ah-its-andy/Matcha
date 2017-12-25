package essentials

import (
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
)

type ScriptResources struct {
	s sync.Map
}

func (r *ScriptResources) GetResource(name string) ([]byte, error) {
	v, ok := r.s.Load(name)
	if ok {
		return v.([]byte), nil
	}
	return nil, fmt.Errorf("resource %s not found", name)
}

func (r *ScriptResources) GetResources() map[string][]byte {
	result := make(map[string][]byte)
	forEach := func(key interface{}, value interface{}) bool {
		result[key.(string)] = value.([]byte)
		return true
	}
	r.s.Range(forEach)
	return result
}

func (r *ScriptResources) Store(key string, data string) error {
	d, err := base64.StdEncoding.DecodeString(data)
	str := strings.Replace(string(d), "	", "  ", -1)
	if err != nil {
		return err
	}
	r.s.Store(key, []byte(str))
	return nil
}
