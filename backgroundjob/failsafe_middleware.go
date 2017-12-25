package backgroundjob

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/standardcore/Matcha/essentials"
	"github.com/streadway/amqp"
)

type FailSafeMiddleware struct {
	*essentials.RabbitConsumeMiddleware
}

func NewFailSafeMiddleware(sess *essentials.Session) essentials.Middleware {
	r := &FailSafeMiddleware{}
	r.RabbitConsumeMiddleware = essentials.NewRabbitConsumeMiddleware(sess, r)
	return r
}

func (m *FailSafeMiddleware) OnConsume(ctx *essentials.MatchaContext, channel *amqp.Channel) error {
	resolve, err := ctx.GetSession().ResolveRef("$backgroundjob_failsafe")
	if err != nil {
		return err
	}
	ctx.GetSession().TryExchangeDeclare("backgroundjob_exchange")
	ctx.GetSession().TryQueueDeclare("backgroundjob_failsafe")
	d, err := channel.Consume(resolve, "matcha backgroundjob failsafe", false, false, false, false, make(amqp.Table))
	if err != nil {
		return err
	}

	go m.ListenDelivery(d)

	return nil
}

func (m *FailSafeMiddleware) OnDelivery(ctx *essentials.MatchaContext, channel *amqp.Channel, args *essentials.ConsumerDeliverEventArgs) error {
	var payload essentials.Payload
	err := json.Unmarshal(args.Body, &payload)
	if err != nil {
		_ = channel.Nack(args.DeliveryTag, false, false)
		return nil
	}
	delay, ok := payload.Extensions["delay"]
	if ok == false {
		_ = channel.Nack(args.DeliveryTag, false, false)
		return nil
	}
	delaySeconds, err := strconv.ParseInt(delay, 10, 32)
	if err != nil {
		_ = channel.Nack(args.DeliveryTag, false, false)
		return nil
	}
	dbConn, err := ctx.GetSession().CreateConnectionFactory().Database()
	if err != nil {
		_ = channel.Nack(args.DeliveryTag, false, true)
		return err
	}
	defer dbConn.Close()
	transact, err := dbConn.BeginTx(sql.LevelReadCommitted)
	if err != nil {
		_ = channel.Nack(args.DeliveryTag, false, true)
		return err
	}

	msg, err := essentials.AppendMessage(&payload, HandlePayloadExtension, transact)
	if err != nil {
		_ = channel.Nack(args.DeliveryTag, false, true)
		return err
	}
	atJob := ctx.GetSession().CreateAtJobFactory().New(msg.ID)
	spec := fmt.Sprintf("now + %d seconds", delaySeconds)

	if delaySeconds > 0 {
		err = msg.ChangeState(essentials.MessageProcessing, transact)
		if err != nil {
			_ = channel.Nack(args.DeliveryTag, false, true)
			return nil
		}
		err = GetScheduler().AddJob(spec, atJob)
		if err != nil {
			_ = channel.Nack(args.DeliveryTag, false, false)
			return nil
		}
	}

	err = transact.Commit()
	if err != nil {
		_ = channel.Nack(args.DeliveryTag, false, true)
		return err
	}
	_ = channel.Ack(args.DeliveryTag, false)
	return nil
}
