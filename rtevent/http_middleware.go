package rtevent

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/streadway/amqp"

	"github.com/standardcore/Matcha/essentials"
)

func ExecutePublishEvent(content []byte, request *http.Request, sess *essentials.Session) error {
	// content, err := ioutil.ReadAll(m.Body)
	// if err != nil {
	// 	m.WriteHeader(406)
	// 	m.ResponseWriter.Write([]byte(err.Error()))
	// 	return err
	// }

	var payload essentials.Payload
	err := json.Unmarshal(content, &payload)
	if err != nil {
		return err
	}

	// if payload.ClientTag == "" {
	// 	payload.ClientTag = request.Header.Get("X-matcha-ClientName")
	// }
	// if payload.ClientTag == "" {
	// 	err = fmt.Errorf("HTTP header `%s` could not be null or empty", "X-matcha-ClientName")
	// 	return err
	// }
	// payload.Env = request.Header.Get("X-matcha-Env")
	// if payload.Env != "dev" && payload.Env != "staging" && payload.Env != "pro" {
	// 	err = fmt.Errorf("HTTP header `%s` should be one of `%s`, `%s`, `%s`", "X-matcha-Env", "dev", "staging", "pro")
	// 	return err
	// }

	dbConn, err := sess.CreateConnectionFactory().Database()
	if err != nil {
		return err
	}
	defer dbConn.Close()
	transact, err := dbConn.BeginTx(sql.LevelReadCommitted)
	if err != nil {
		return err
	}
	defer transact.Rollback()

	msgid, err := publishEventWriteDb(sess, &payload, transact)
	if err != nil {
		return err
	}

	deliveryMsg := &essentials.DeliveryMessage{
		MessageID:   msgid,
		MessageType: payload.MessageType,
		Content:     payload.Content,
		PublishTime: time.Now().Unix(),
		Extensions:  make(map[string]string),
	}
	deliveryMsg.Extensions["x-matcha-tag"] = payload.ClientTag

	exchange, ok := payload.Extensions["x-event-exchange"]
	if !ok {
		err = errors.New("key `x-event-exchange` not found in extensions")
		return err
	}

	routeKey, ok := payload.Extensions["x-event-routekey"]
	if !ok {
		err = errors.New("key `x-event-routekey` not found in extensions")
		return err
	}
	deliveryMsg.Extensions["x-matcha-routekey"] = routeKey

	queue, ok := payload.Extensions["x-event-queue"]
	if ok && queue != "" {
		deliveryMsg.Extensions["x-matcha-queue"] = queue
		routeKey = queue
	}

	err = publishEvent(sess, deliveryMsg, exchange, routeKey)
	if err != nil {
		return err
	}

	err = transact.Commit()
	if err != nil {
		return err
	}

	transact2, err := dbConn.BeginTx(sql.LevelReadCommitted)
	if err != nil {
		return err
	}
	defer transact2.Rollback()

	err = publishEventPublished(msgid, transact2)
	if err != nil {
		return err
	}

	err = transact2.Commit()
	if err != nil {
		return err
	}

	return nil
}

func publishEventWriteDb(sess *essentials.Session, payload *essentials.Payload, executor essentials.DbExecutor) (string, error) {
	msg, err := essentials.AppendMessage(payload, publishWriteEvent, executor)
	if err != nil {
		return "", err
	}
	return msg.ID, nil
}

func publishEventPublished(msgid string, executor essentials.DbExecutor) error {
	msg, err := essentials.FindOneMessage(msgid, true, executor)
	if err != nil {
		return essentials.WrapError("EventBus HTTP", err)
	}

	err = msg.ChangeState(essentials.MessageProcessing, executor)
	if err != nil {
		return essentials.WrapError("EventBus HTTP", err)
	}

	return nil
}

func publishWriteEvent(args *essentials.ExtensionsEventArgs, executor essentials.DbExecutor) error {
	exchange, ok := args.Extensions["x-event-exchange"]
	if !ok {
		return errors.New("key `x-event-exchange` not found in extensions")
	}
	routeKey, ok := args.Extensions["x-event-routekey"]
	if !ok {
		return errors.New("key `x-event-routekey` not found in extensions")
	}
	queue, ok := args.Extensions["x-event-queue"]

	var err error

	if !ok {
		_, err = executor.ExecScript("InsertEvent", essentials.NewOrderedUUID(), args.MessageID, exchange, routeKey, nil)
	} else {
		_, err = executor.ExecScript("InsertEvent", essentials.NewOrderedUUID(), args.MessageID, exchange, routeKey, queue)
	}

	if err != nil {
		return err
	}
	return nil
}

func publishEvent(sess *essentials.Session, payload *essentials.DeliveryMessage, exchange string, routeKey string) error {
	conn, err := sess.CreateConnectionFactory().RabbitMQ()
	if err != nil {
		return err
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	err = channel.Publish(exchange, routeKey, false, false, amqp.Publishing{
		Body: body,
	})
	if err != nil {
		return err
	}
	return nil
}
