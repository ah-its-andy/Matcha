package essentials

import (
	"database/sql"
	"encoding/json"

	"github.com/streadway/amqp"
)

type RollbackMessageProcessor struct {
	sess *Session
}

func NewRollbackMessageProcessor(sess *Session) Processor {
	return &RollbackMessageProcessor{sess: sess}
}

func (p *RollbackMessageProcessor) Process() error {
	//enablishing mq connection
	mqConn, err := p.sess.CreateConnectionFactory().RabbitMQ()
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}
	defer mqConn.Close()
	channel, err := mqConn.Channel()
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}
	defer channel.Close()
	err = channel.ExchangeDeclare("rollback@exchange.matcha.message", "direct", true, false, false, false, make(amqp.Table))
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}
	err = channel.Tx()
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}
	defer channel.TxRollback()
	//enablishing db connection
	conn, err := p.sess.CreateConnectionFactory().Database()
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}
	defer conn.Close()
	transact, err := conn.BeginTx(sql.LevelReadCommitted)
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}
	defer transact.Rollback()
	row, err := transact.QueryScriptRow("FindOneRollbackMessage")
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}

	var payload DeliveryMessage
	var exchange, routeKey, queue, publisher string
	err = row.Scan(&payload.MessageID, &payload.MessageType, &publisher, &payload.Content, &routeKey, &queue, &exchange)
	if err == sql.ErrNoRows {
		p.sess.Logger().Debugln(WrapError("RollbackMessageProcessor", err))
		return nil
	} else if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}

	payload.Extensions = make(map[string]string)
	payload.Extensions["x-matcha-exchange"] = exchange
	if queue != "" {
		payload.Extensions["x-matcha-queue"] = queue
	}
	payload.Extensions["x-matcha-routekey"] = routeKey
	payload.Extensions["x-matcha-publisher"] = publisher
	payload.Extensions["x-matcha-tag"] = "rollback_processor"

	content, err := json.Marshal(&payload)
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}

	msg := &Message{ID: payload.MessageID, State: MessageFailed, StateName: MessageFailed.String()}
	err = msg.ChangeState(MessageRollback, transact)
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}

	subs, err := msg.FetchSubscriptions(transact)
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}

	pub := amqp.Publishing{
		Body: content,
	}

	for _, sub := range subs {
		err = channel.Publish("rollback@exchange.matcha.message", sub.ReceiverTag, false, false, pub)
		if err != nil {
			return WrapError("RollbackMessageProcessor", err)
		}
		//只回滚Success
		// if sub.StateName != "Failed" {
		// 	err = channel.Publish("rollback@matcha.message", sub.ReceiverTag, false, false, pub)
		// 	if err != nil {
		// 		return WrapError("RollbackMessageProcessor", err)
		// 	}
		// }
	}
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}

	err = transact.Commit()
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}

	err = channel.TxCommit()
	if err != nil {
		return WrapError("RollbackMessageProcessor", err)
	}

	return ProcessorWaitNext
}
