package essentials

import "github.com/streadway/amqp"

type ConsumeMiddleware interface {
	Middleware

	OnConsume(ctx *MatchaContext, channel *amqp.Channel) error

	OnDelivery(ctx *MatchaContext, channel *amqp.Channel, args *ConsumerDeliverEventArgs) error

	OnError(ctx *MatchaContext, args *ConsumerDeliverEventArgs, err error)
}

type ConsumerDeliverEventArgs struct {
	*amqp.Delivery
}
