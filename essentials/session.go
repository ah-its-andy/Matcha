package essentials

import (
	"fmt"
	"sync"

	"github.com/standardcore/go-logging"
	ymsql "github.com/standardcore/go-ymsql"
	"github.com/streadway/amqp"
)

//work above go 1.9
type Session struct {
	parameters   *Parameters
	declarations *DeclarationMap
	scripts      ymsql.Store
	res          *ScriptResources
	logger       logging.Logger
}

func NewSession(parameters map[string]string, declarations *DeclarationsConfig) (*Session, error) {
	sess := &Session{
		parameters:   &Parameters{},
		declarations: &DeclarationMap{},
		scripts:      ymsql.NewYMLStore(),
		res:          NewScriptResources(),
		logger:       logging.NewLogger(),
	}
	for k, v := range parameters {
		sess.parameters.Store(k, v)
	}
	sess.declarations.sess = sess
	if declarations != nil {
		sess.declarations.ConfigureExchangeDeclarations(declarations.Exchanges)
		sess.declarations.ConfigureQueueDeclarations(declarations.Queues)
	}
	// scriptDirectory, ok := sess.parameters.Load("scripts")
	// if ok {
	// 	err := sess.scripts.StoreFromDirectory(scriptDirectory.(string))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }
	var wg sync.WaitGroup
	if res := sess.res.GetResources(); res != nil && len(res) > 1 {
		wg.Add(len(res))
		for k, v := range res {
			err := sess.scripts.Store(v)
			if err != nil {
				return nil, WrapError(fmt.Sprintf("NewSession:%s", k), err)
			}
			wg.Done()
		}
	}
	wg.Wait()

	schema, err := sess.Require("dbprefix")
	if err != nil {
		return nil, err
	}
	sess.scripts.SETEnv("SCHEMA", schema)
	return sess, nil
}

func (sess *Session) SETLogger(logger logging.Logger) {
	sess.logger = logger
}

func (sess *Session) Logger() logging.Logger {
	return sess.logger
}

func (sess *Session) Script(name string) (ymsql.Scripting, error) {
	return sess.scripts.Load(name)
}

func (sess *Session) Parameters() *Parameters {
	return sess.parameters
}

func (sess *Session) Load(key string) (string, bool) {
	v, ok := sess.parameters.Load(key)
	return v.(string), ok
}

func (sess *Session) LoadOrEmpty(key string) string {
	return sess.parameters.LoadOrEmpty(key)
}

func (sess *Session) Require(key string) (string, error) {
	return sess.parameters.Require(key)
}

func (sess *Session) ResolveRef(ref string) (string, error) {
	return sess.parameters.ResolveRef(ref)
}

func (sess *Session) GetExchange(key string) (*Exchange, error) {
	return sess.declarations.GetExchange(key)
}

func (sess *Session) GetQueue(key string) (*Queue, error) {
	return sess.declarations.GetQueue(key)
}

func (sess *Session) ExchangeDeclare(key string) error {
	factory := sess.CreateConnectionFactory()
	conn, err := factory.RabbitMQ()
	if err != nil {
		return WrapError("Session.ExchangeDeclare", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return WrapError("Session.ExchangeDeclare", err)
	}
	defer channel.Close()

	return sess.declarations.ExchangeDeclare(channel, key)
}

func (sess *Session) QueueDeclare(key string) (amqp.Queue, error) {
	factory := sess.CreateConnectionFactory()
	conn, err := factory.RabbitMQ()
	if err != nil {
		return amqp.Queue{}, WrapError("Session.QueueDeclare", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return amqp.Queue{}, WrapError("Session.QueueDeclare", err)
	}
	defer channel.Close()
	return sess.declarations.QueueDeclare(channel, key)
}

func (sess *Session) TryExchangeDeclare(key string) {
	sess.declarations.TryExchangeDeclare(sess.CreateConnectionFactory(), key)
}

func (sess *Session) TryQueueDeclare(key string) {
	sess.declarations.TryQueueDeclare(sess.CreateConnectionFactory(), key)
}

//-------- helpers
func (sess *Session) CreateAtJobFactory() *AtJobFactory {
	return &AtJobFactory{sess: sess}
}

func (sess *Session) CreateConnectionFactory() *ConnectionFactory {
	return &ConnectionFactory{sess: sess}
}
