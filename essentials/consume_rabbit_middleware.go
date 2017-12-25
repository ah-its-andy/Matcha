package essentials

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type RabbitConsumeMiddleware struct {
	ConsumeMiddleware

	sess *Session

	running bool

	connection *amqp.Connection
	channel    *amqp.Channel

	connCloseReceiver    chan *amqp.Error
	channelCloseReceiver chan *amqp.Error

	mu sync.Mutex
}

func NewRabbitConsumeMiddleware(sess *Session, middleware ConsumeMiddleware) *RabbitConsumeMiddleware {
	return &RabbitConsumeMiddleware{
		sess:              sess,
		running:           false,
		ConsumeMiddleware: middleware,
	}
}

func (m *RabbitConsumeMiddleware) Execute(ctx *MatchaContext) error {
	err := m.revive()
	if err != nil {
		return err
	}
	return nil
}

func (m *RabbitConsumeMiddleware) ListenDelivery(c <-chan amqp.Delivery) {
	for delivery := range c {
		args := &ConsumerDeliverEventArgs{
			Delivery: &delivery,
		}
		ctx := m.getContext()
		err := m.ConsumeMiddleware.OnDelivery(ctx, m.channel, args)
		if err != nil {
			m.ConsumeMiddleware.OnError(ctx, args, err)
		}
	}
}

func (m *RabbitConsumeMiddleware) getContext() *MatchaContext {
	return &MatchaContext{
		sess: m.sess,
	}
}

func (m *RabbitConsumeMiddleware) ensureConnection() error {
	connection, err := m.sess.CreateConnectionFactory().RabbitMQ()
	if err != nil {
		return err
	}
	m.connection = connection
	m.connection.NotifyClose(m.connCloseReceiver)
	return nil
}

func (m *RabbitConsumeMiddleware) ensureChannel() error {
	channel, err := m.connection.Channel()
	if err != nil {
		return err
	}
	m.channel = channel
	m.channel.NotifyClose(m.channelCloseReceiver)
	return nil
}

func (m *RabbitConsumeMiddleware) setRunning(v bool) {
	m.mu.Lock()
	m.running = v
	m.mu.Unlock()
}

func (m *RabbitConsumeMiddleware) getRunning() bool {
	m.mu.Lock()
	running := m.running
	m.mu.Unlock()
	return running
}

func (m *RabbitConsumeMiddleware) ensureClose() {
	m.setRunning(false)
	if m.channel != nil {
		m.channel.Close()
	}
	if m.connection != nil {
		m.connection.Close()
	}
}

func (m *RabbitConsumeMiddleware) revive() error {
	m.ensureClose()

	err := m.ensureConnection()
	if err != nil {
		return err
	}
	err = m.ensureChannel()
	if err != nil {
		return err
	}
	m.listenClose()
	err = m.ConsumeMiddleware.OnConsume(m.getContext(), m.channel)
	if err != nil {
		return err
	}

	m.setRunning(true)

	return nil
}

func (m *RabbitConsumeMiddleware) infiniteRevive() {
	if !m.getRunning() {
		return
	}
	m.setRunning(false)
	for {
		err := m.revive()
		if err == nil {
			goto ForEnd
		}
		time.Sleep(time.Second * 15)
	}
ForEnd:
}

func (m *RabbitConsumeMiddleware) listenClose() {
	go func() {
		for {
			select {
			case <-m.connCloseReceiver:
				m.infiniteRevive()
			}
		}
	}()
	go func() {
		for {
			select {
			case <-m.channelCloseReceiver:
				m.infiniteRevive()
			}
		}
	}()
}
