package essentials

import (
	"sync"
)

type MatchaContext struct {
	sess   *Session
	wg     sync.WaitGroup
	values sync.Map
}

func NewMatchaContext(sess *Session) *MatchaContext {
	return &MatchaContext{
		sess: sess,
	}
}

func (c *MatchaContext) GetSession() *Session {
	return c.sess
}

func (c *MatchaContext) ScheduleOne() {
	c.wg.Add(1)
}

func (c *MatchaContext) Done() {
	c.wg.Done()
}

func (c *MatchaContext) Wait() {
	c.wg.Wait()
}

func (c *MatchaContext) AddValue(key interface{}, value interface{}) {
	c.values.Store(key, value)
}

func (c *MatchaContext) Value(key interface{}) (v interface{}, ok bool) {
	return c.values.Load(key)
}
