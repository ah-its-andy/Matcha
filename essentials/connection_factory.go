package essentials

import (
	"database/sql"

	"github.com/streadway/amqp"
)

type ConnectionFactory struct {
	sess *Session
}

func (factory *ConnectionFactory) Database() (*DbConnection, error) {
	dbDriverName, err := factory.sess.Require("db_driver_name")
	if err != nil {
		return nil, WrapError("ConnectionFactory.Database", err)
	}
	dbConnStr, err := factory.sess.Require("datasource")
	if err != nil {
		return nil, WrapError("ConnectionFactory.Database", err)
	}
	conn, err := sql.Open(dbDriverName, dbConnStr)
	if err != nil {
		return nil, WrapError("ConnectionFactory.Database", err)
	}
	return &DbConnection{internalConnection: conn, sess: factory.sess}, nil
}

func (factory *ConnectionFactory) RabbitMQ() (*amqp.Connection, error) {
	rabbitMQURI, err := factory.sess.Require("rabbitmq")
	if err != nil {
		return nil, WrapError("ConnectionFactory.Database", err)
	}
	conn, err := amqp.Dial(rabbitMQURI)
	if err != nil {
		return nil, WrapError("ConnectionFactory.Database", err)
	}
	return conn, nil
}
