package essentials

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Jamesxql/at"

	"github.com/streadway/amqp"
)

type Scheduler struct {
	a *at.At
}

var scheduler *Scheduler

func GetRetryScheduler() *Scheduler {
	if scheduler == nil {
		scheduler = NewScheduler()
	}
	return scheduler
}

func NewScheduler() *Scheduler {
	scheduler := &Scheduler{
		a: at.New(),
	}
	scheduler.a.Start()
	return scheduler
}

func (s *Scheduler) AddJob(spec string, job *AtJob) error {
	//Debug(spec + " scheduled")
	return s.a.AddJobWithID(job.messageID, spec, job)
}

type AtJobFactory struct {
	sess *Session
}

func (factory *AtJobFactory) New(messageID string) *AtJob {
	return &AtJob{
		sess:      factory.sess,
		messageID: messageID,
		factory:   factory.sess.CreateConnectionFactory(),
	}
}

type AtJob struct {
	sess      *Session
	messageID string
	factory   *ConnectionFactory
}

func (job *AtJob) Run() {
	//ctx, cancel := context.WithTimeout(context.Background(), time.Minute*30)

	err := job.run()
	if err != nil {
		job.sess.Logger().Error(err)
	}
}

func (job *AtJob) run() error {
	job.sess.Logger().Errorln("Scheduled at job starting," + job.messageID)

	var channel *amqp.Channel

	dbConn, err := job.factory.Database()
	if err != nil {
		return err
	}
	defer dbConn.Close()

	transact, err := dbConn.BeginTx(sql.LevelReadCommitted)
	if err != nil {
		return err
	}
	defer transact.Rollback()

	msg, err := FindOneMessage(job.messageID, true, transact)
	if err != nil {
		return err
	}

	if msg == nil || msg.ID == "" {
		return nil
	}

	subs, err := msg.FetchSubscriptions(transact)
	if err != nil {
		return err
	}

	deliveryMsg := &DeliveryMessage{
		MessageID:   msg.ID,
		MessageType: msg.MessageType,
		Content:     msg.Content,
		PublishTime: time.Now().Unix(),
		Extensions:  make(map[string]string),
	}

	deliveryMsg.Extensions["x-matcha-tag"] = msg.Publisher

	body, err := json.Marshal(deliveryMsg)
	if err != nil {
		return err
	}

	p := amqp.Publishing{
		Body: body,
	}

	ttl := job.sess.LoadOrEmpty("message_ttl")
	if ttl != "" {
		p.Expiration = ttl
	}

	amqpConn, err := job.factory.RabbitMQ()
	if err != nil {
		goto Return
	}

	defer amqpConn.Close()

	channel, err = amqpConn.Channel()
	if err != nil {
		goto Return
	}

	for _, sub := range subs {
		if sub.StateName == "Scheduled" {
			tempConn, err := job.factory.Database()
			if err != nil {
				goto Return
			}
			job.sess.Logger().Infoln(fmt.Sprintf("Exchange : %s ; Key : %s ", sub.Exchange, sub.RouteKey))
			err = channel.Publish(sub.Exchange, sub.RouteKey, false, false, p)
			if err != nil {
				tempConn.Close()
				goto Return
			}
			err = sub.ChangeState("Published", tempConn)
			if err != nil {
				tempConn.Close()
				goto Return
			}
			tempConn.Close()
		}
	}

	err = msg.Published(transact)
	if err != nil {
		return err
	}

	err = transact.Commit()
	if err != nil {
		return err
	}

Return:
	if err != nil {
		if msg.Retry >= 10 {
			_ = msg.ChangeState(MessageFailed, transact)
		} else if retry, _ := msg.IncreaseRetry(transact); retry > 0 && retry < 10 {
			_ = GetRetryScheduler().AddJob("now + 15 seconds", job)
		}
		return err
	}
	return nil
}

type DeliveryMessage struct {
	MessageID   string            `json:"message_id"`
	MessageType string            `json:"message_type"`
	Content     string            `json:"content"`
	PublishTime int64             `json:"publish_time"`
	Extensions  map[string]string `json:"exts"`
}
