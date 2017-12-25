package essentials

import (
	"fmt"
	"strings"
	"sync"
)

type Parameters struct {
	sync.Map
}

func (p *Parameters) LoadOrEmpty(key string) string {
	v, ok := p.Load(key)
	if ok {
		return v.(string)
	}
	return ""
}

func (p *Parameters) ResolveRef(ref string) (string, error) {
	if strings.HasPrefix(ref, "$") {
		end := len(ref)
		key := ref[1:end]
		v, ok := p.Load(key)
		if ok {
			// p.sess.Logger().Infoln(fmt.Sprintf("ResolveRef %s(%s) : %s", ref, key, v.(string)))
			return v.(string), nil
		}
		return "", fmt.Errorf("ref key %s not found in parameters", key)
	}

	return ref, nil
}

func (p *Parameters) Require(key string) (string, error) {
	v, ok := p.Load(key)
	if ok == false {
		return "", NotFoundError(key)
	}
	return v.(string), nil
}

func (p *Parameters) LoadObject(target interface{}, key string) error {
	v, err := p.Require(key)
	if err != nil {
		return err
	}
	_ = v
	return nil
}
