package api

import (
	"database/sql"
	"encoding/json"

	"github.com/standardcore/Matcha/essentials"
)

func ExecuteListEvents(content []byte, sess *essentials.Session) ([]byte, error) {
	var parameter ListEventMessagesParameters
	err := json.Unmarshal(content, &parameter)
	if err != nil {
		return nil, err
	}
	conn, err := sess.CreateConnectionFactory().Database()
	if err != nil {
		return nil, err
	}
	result, err := ListEventMessages(&parameter, conn, sess)
	if err != nil {
		return nil, err
	}
	if result == nil || len(result) == 0 {
		return nil, nil
	}
	buffer, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func ExecuteListJobs(content []byte, sess *essentials.Session) ([]byte, error) {
	var parameter JobParameters
	err := json.Unmarshal(content, &parameter)
	if err != nil {
		return nil, err
	}
	conn, err := sess.CreateConnectionFactory().Database()
	if err != nil {
		return nil, err
	}
	result, err := ListJobMessages(&parameter, conn, sess)
	if err != nil {
		return nil, err
	}
	if result == nil || len(result) == 0 {
		return nil, nil
	}
	buffer, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func ExecuteGetMessageContent(messageID string, sess *essentials.Session) ([]byte, error) {
	conn, err := sess.CreateConnectionFactory().Database()
	if err != nil {
		return nil, err
	}
	row, err := conn.QueryScriptRow("FindOneMessage", messageID)
	if sql.ErrNoRows == err {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var message essentials.Message
	err = row.Scan(&message.ID, &message.MessageType, &message.Content, &message.State,
		&message.StateName, &message.Retry, &message.CreationTime, &message.CreationTimeString,
		&message.Env)
	if err != nil {
		return nil, err
	}
	return []byte(message.Content), nil
}
