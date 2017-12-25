package backgroundjob

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/standardcore/Matcha/essentials"
	"github.com/streadway/amqp"
)

func ExecuteAddJob(content []byte, request *http.Request, sess *essentials.Session) (string, error) {
	//content, err := ioutil.ReadAll(m.Body)
	// if err != nil {
	// 	m.WriteHeader(406)
	// 	m.ResponseWriter.Write([]byte(err.Error()))
	// 	return err
	// }
	var payload essentials.Payload
	err := json.Unmarshal(content, &payload)
	if err != nil {
		return "", err
	}

	// if payload.ClientTag == "" {
	// 	payload.ClientTag = request.Header.Get("X-matcha-ClientName")
	// }
	// if payload.ClientTag == "" {
	// 	err = fmt.Errorf("HTTP header `%s` could not be null or empty", "X-matcha-ClientName")
	// 	return "", err
	// }
	// payload.Env = request.Header.Get("X-matcha-Env")
	// if payload.Env != "dev" && payload.Env != "staging" && payload.Env != "pro" {
	// 	err = fmt.Errorf("HTTP header `%s` should be one of `%s`, `%s`, `%s`", "X-matcha-Env", "dev", "staging", "pro")
	// 	return "", err
	// }

	msgid, err := addJobWriteDb(sess, &payload)
	if err != nil {
		return "", err
	}
	return msgid, nil
}

func addJobWriteDb(sess *essentials.Session, payload *essentials.Payload) (string, error) {
	dbConn, err := sess.CreateConnectionFactory().Database()
	if err != nil {
		return "", err
	}
	defer dbConn.Close()
	transact, err := dbConn.BeginTx(sql.LevelReadCommitted)
	if err != nil {
		return "", err
	}
	msg, err := essentials.AppendMessage(payload, HandlePayloadExtension, transact)
	if err != nil {
		return "", err
	}
	err = transact.Commit()
	if err != nil {
		return "", err
	}
	delay, _ := payload.Extensions["delay"]
	delaySeconds, err := strconv.ParseInt(delay, 10, 32)
	if err != nil {
		return "", err
	}
	atJob := sess.CreateAtJobFactory().New(msg.ID)
	spec := fmt.Sprintf("now + %d seconds", delaySeconds)
	if delaySeconds > 0 {
		err = GetScheduler().AddJob(spec, atJob)
		if err != nil {
			return "", err
		}
		err = msg.ChangeState(essentials.MessageProcessing, dbConn)
		if err != nil {
			return "", err
		}
	}
	return msg.ID, nil
}

func addJobTryPublish(sess *essentials.Session, body []byte) {

	publish := &amqp.Publishing{
		Body: body,
	}

	exchange, ok := sess.Load("backgroundjob_exchange")
	if ok == false {
		return
	}
	failsafe, ok := sess.Load("backgroundjob_failsafe")
	if ok == false {
		return
	}

	forFunc := func() error {
		conn, err := sess.CreateConnectionFactory().RabbitMQ()
		if err != nil {
			return err
		}
		defer conn.Close()
		channel, err := conn.Channel()
		if err != nil {
			return err
		}
		return channel.Close()
		err = sess.ExchangeDeclare("backgroundjob_exchange")
		if err != nil {
			return err
		}
		_, err = sess.QueueDeclare("backgroundjob_failsafe")
		//_, err = channel.QueueDeclare(failsafe.(string), true, false, false, false, make(amqp.Table))
		if err != nil {
			return err
		}
		//err = channel.QueueBind(failsafe.(string), failsafe.(string), exchange.(string), false, make(amqp.Table))
		//if err != nil {
		//	goto WaitNextRound
		//}
		err = channel.Publish(exchange, failsafe, false, false, *publish)
		if err != nil {
			return err
		}
		return nil
	}
	sess.Logger().Infoln("backgroundjob.serverhandler : failsafe : try publishing")
	for {
		err := forFunc()
		if err != nil {
			sess.Logger().Errorln(essentials.WrapError("backgroundjob.serverhandler : failsafe : ", err))
			goto WaitNextRound
		}
		goto ForEnd

	WaitNextRound:
		sess.Logger().Infoln("backgroundjob.serverhandler : failsafe : waiting")
		time.Sleep(time.Second * 30)
	}
ForEnd:
	sess.Logger().Infoln("backgroundjob.serverhandler : failsafe : succeeded")
}
