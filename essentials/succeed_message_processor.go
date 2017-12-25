package essentials

import (
	"database/sql"
	"time"
)

type SucceedMessageProcessor struct {
	sess *Session
}

func NewSucceedMessageProcessor(sess *Session) Processor {
	return &SucceedMessageProcessor{sess: sess}
}

func (p *SucceedMessageProcessor) Process() error {
	conn, err := p.sess.CreateConnectionFactory().Database()
	if err != nil {
		return WrapError("SucceedMessageProcessor", err)
	}
	defer conn.Close()
	transact, err := conn.BeginTx(sql.LevelReadCommitted)
	if err != nil {
		return WrapError("SucceedMessageProcessor", err)
	}
	defer transact.Rollback()
	row, err := transact.QueryScriptRow("FindOneSucceedMessage", time.Now().Add(time.Minute*-1).Unix())
	if err != nil {
		return WrapError("SucceedMessageProcessor", err)
	}
	var message Message
	err = row.Scan(&message.ID, &message.MessageType, &message.Content,
		&message.State, &message.StateName, &message.Retry,
		&message.CreationTime, &message.CreationTimeString,
		&message.Publisher, &message.PublishTime, &message.PublishTimeString,
		&message.Env)
	if sql.ErrNoRows == err {
		p.sess.Logger().Debugln(WrapError("SucceedMessageProcessor", err))
		return nil
	} else if err != nil {
		return WrapError("SucceedMessageProcessor", err)
	}
	err = message.ChangeState(MessageSucceeded, transact)
	if err != nil {
		return WrapError("SucceedMessageProcessor", err)
	}
	err = transact.Commit()
	if err != nil {
		return WrapError("SucceedMessageProcessor", err)
	}

	p.sess.Logger().Infof("message %s succeeded", message.ID)

	return ProcessorWaitNext
}
