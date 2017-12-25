package essentials

import (
	"database/sql"
	"time"
)

type FailedMessageProcessor struct {
	sess *Session
}

func NewFailedMessageProcessor(sess *Session) Processor {
	return &FailedMessageProcessor{sess: sess}
}

func (p *FailedMessageProcessor) Process() error {
	conn, err := p.sess.CreateConnectionFactory().Database()
	if err != nil {
		return WrapError("FailedMessageProcessor", err)
	}
	defer conn.Close()
	transact, err := conn.BeginTx(sql.LevelReadCommitted)
	if err != nil {
		return WrapError("FailedMessageProcessor", err)
	}
	defer transact.Rollback()
	row, err := transact.QueryScriptRow("FindOneFailedMessage", time.Now().Add(time.Minute*-2).Unix())
	if err != nil {
		return WrapError("FailedMessageProcessor", err)
	}
	var msg Message
	err = row.Scan(&msg.ID, &msg.MessageType, &msg.Content,
		&msg.State, &msg.StateName, &msg.Retry, &msg.CreationTime,
		&msg.CreationTimeString, &msg.Publisher, &msg.PublishTime,
		&msg.PublishTimeString, &msg.Env)
	if err == sql.ErrNoRows {
		p.sess.Logger().Debugln(WrapError("FailedMessageProcessor", err))
		return nil
	} else if err != nil {
		return WrapError("FailedMessageProcessor", err)
	}
	err = msg.ChangeState(MessageFailed, transact)
	if err != nil {
		return WrapError("FailedMessageProcessor", err)
	}
	err = transact.Commit()
	if err != nil {
		return WrapError("FailedMessageProcessor", err)
	}

	p.sess.Logger().Infof("message %s failed", msg.ID)

	return ProcessorWaitNext
}
