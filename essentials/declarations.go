package essentials

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

type DeclarationsConfig struct {
	Exchanges map[string]*Exchange `json:"exchanges"`
	Queues    map[string]*Queue    `json:"queues"`
}

func NewDeclarationsConfig() *DeclarationsConfig {
	return &DeclarationsConfig{
		Exchanges: make(map[string]*Exchange),
		Queues:    make(map[string]*Queue),
	}
}

func (cnf *DeclarationsConfig) Fanout(factory *ConnectionFactory) error {
	conn, err := factory.RabbitMQ()
	if err != nil {
		return err
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	err = channel.ExchangeDeclare("declaration@matcha.fanout", "fanout", true, false, false, false, make(amqp.Table))
	if err != nil {
		return err
	}
	body, err := json.Marshal(cnf)
	if err != nil {
		return err
	}
	msg := amqp.Publishing{
		DeliveryMode: 2,
		Body:         body,
	}
	err = channel.Publish("declaration@matcha.fanout", "*", false, false, msg)
	if err != nil {
		return err
	}
	return nil
}

type DeclarationMap struct {
	sess      *Session
	exchanges sync.Map
	queues    sync.Map
}

func (m *DeclarationMap) GetExchangeMap() *sync.Map {
	return &m.exchanges
}

func (m *DeclarationMap) ConfigureQueueDeclarations(source map[string]*Queue) {
	if source == nil {
		return
	}
	for k, v := range source {
		m.queues.Store(k, v.Refill())
	}
}

func (m *DeclarationMap) ConfigureExchangeDeclarations(source map[string]*Exchange) {
	if source == nil {
		return
	}
	for k, v := range source {
		m.exchanges.Store(k, v.Refill())
	}
}

func (m *DeclarationMap) completeRefString(key string, ref string) string {
	if ref == "$" {
		return fmt.Sprintf("$%s", key)
	}
	return ref
}

func (m *DeclarationMap) completeTable(key string, s amqp.Table) (amqp.Table, error) {
	if s == nil || len(s) == 0 {
		return s, nil
	}
	var err error
	for k, v := range s {
		value, ok := v.(string)
		if ok {
			s[k], err = m.sess.ResolveRef(m.completeRefString(key, value))
			if err != nil {
				return s, err
			}
		}
	}
	return s, nil
}

func (m *DeclarationMap) GetExchange(key string) (*Exchange, error) {
	v, ok := m.exchanges.Load(key)
	if ok {
		e := v.(*Exchange)
		var err error
		e.Name, err = m.sess.ResolveRef(m.completeRefString(key, e.Name))
		if err != nil {
			return nil, err
		}
		e.Arguments, err = m.completeTable(key, e.Arguments)
		if err != nil {
			return nil, err
		}

		return e, nil
	}
	return nil, fmt.Errorf("exchange key %s not found in declarations section", key)
}

func (m *DeclarationMap) GetQueue(key string) (*Queue, error) {
	v, ok := m.queues.Load(key)
	if ok {
		q := v.(*Queue)
		var err error
		q.Name, err = m.sess.ResolveRef(m.completeRefString(key, q.Name))
		if err != nil {
			return nil, err
		}
		q.Arguments, err = m.completeTable(key, q.Arguments)
		if err != nil {
			return nil, err
		}

		return v.(*Queue), nil
	}
	return nil, fmt.Errorf("queue key %s not found in declarations section", key)
}

func (m *DeclarationMap) ExchangeDeclare(channel *amqp.Channel, key string) error {
	e, err := m.GetExchange(key)
	if err != nil {
		return err
	}
	return channel.ExchangeDeclare(e.Name, e.Type, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Arguments)
}

func (m *DeclarationMap) QueueDeclare(channel *amqp.Channel, key string) (amqp.Queue, error) {
	q, err := m.GetQueue(key)
	if err != nil {
		return amqp.Queue{}, err
	}
	queue, err := channel.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.Arguments)
	if err != nil {
		return queue, err
	}
	if len(q.Bindings) == 0 {
		return queue, nil
	}
	for _, b := range q.Bindings {
		err = channel.QueueBind(q.Name, b.RouteKey, b.Exchange, b.NoWait, b.Arguments)
		if err != nil {
			return queue, err
		}
	}
	return queue, nil
}

func (m *DeclarationMap) TryExchangeDeclare(factory *ConnectionFactory, key string) {
	conn, err := factory.RabbitMQ()
	if err != nil {
		return
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		return
	}
	defer channel.Close()
	_ = m.ExchangeDeclare(channel, key)
}

func (m *DeclarationMap) TryQueueDeclare(factory *ConnectionFactory, key string) {
	conn, err := factory.RabbitMQ()
	if err != nil {
		return
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		return
	}
	defer channel.Close()
	_, _ = m.QueueDeclare(channel, key)
}

type QueueBinding struct {
	RouteKey  string     `json:"route_key"`
	Exchange  string     `json:"exchange"`
	NoWait    bool       `json:"no_wait"`
	Arguments amqp.Table `json:"args"`
}

type Queue struct {
	Name       string          `json:"name"`
	Durable    bool            `json:"durable"`
	AutoDelete bool            `json:"auto_delete"`
	Exclusive  bool            `json:"exclusive"`
	NoWait     bool            `json:"no_wait"`
	Arguments  amqp.Table      `json:"args"`
	Bindings   []*QueueBinding `json:"bindings"`
}

func (q *Queue) Refill() *Queue {
	r := &Queue{
		Name:       q.Name,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Arguments:  make(amqp.Table),
		Bindings:   make([]*QueueBinding, 0),
	}

	if q.Durable == false {
		r.Durable = false
	}
	if q.AutoDelete {
		r.AutoDelete = true
	}
	if q.Exclusive {
		r.Exclusive = true
	}
	if q.NoWait {
		r.NoWait = true
	}
	if q.Arguments != nil && len(q.Arguments) > 0 {
		r.Arguments = q.Arguments
	}

	if q.Bindings == nil || len(q.Bindings) == 0 {
		return r
	}

	for _, b := range q.Bindings {
		binding := &QueueBinding{
			RouteKey:  q.Name,
			Exchange:  b.Exchange,
			NoWait:    false,
			Arguments: make(amqp.Table),
		}
		if b.RouteKey != "" {
			binding.RouteKey = b.RouteKey
		}
		if b.Arguments != nil && len(b.Arguments) > 0 {
			binding.Arguments = b.Arguments
		}
		r.Bindings = append(r.Bindings, binding)
	}

	return r
}

type Exchange struct {
	Name       string     `json:"name"`
	Type       string     `json:"type"`
	Durable    bool       `json:"durable"`
	AutoDelete bool       `json:"auto_delete"`
	Internal   bool       `json:"internal"`
	NoWait     bool       `json:"no_wait"`
	Arguments  amqp.Table `json:"args"`
}

func (e *Exchange) Refill() *Exchange {
	r := &Exchange{
		Name:       e.Name,
		Type:       e.Type,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Arguments:  make(amqp.Table),
	}

	if e.Durable == false {
		r.Durable = e.Durable
	}

	if e.AutoDelete {
		r.AutoDelete = e.AutoDelete
	}

	if e.Internal {
		r.Internal = e.Internal
	}

	if e.NoWait {
		r.NoWait = e.NoWait
	}

	if e.Arguments != nil && len(e.Arguments) > 0 {
		r.Arguments = e.Arguments
	}

	return r
}
