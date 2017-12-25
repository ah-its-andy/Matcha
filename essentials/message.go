package essentials

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

type Message struct {
	ID                 string
	MessageType        string
	Publisher          string
	PublishTime        int64
	PublishTimeString  string
	Content            string
	State              MessageState
	StateName          string
	Retry              int32
	Env                string
	CreationTime       int64
	CreationTimeString string
}

func NewMessage() *Message {
	return &Message{}
}

func (m *Message) FetchLogs(executor DbExecutor) ([]*MessageLog, error) {
	rows, err := executor.QueryScript("FetchMessageLogs", m.ID)
	if err != nil {
		return nil, err
	}
	result := make([]*MessageLog, 0)
	for rows.Next() {
		var log MessageLog
		err = rows.Scan(&log.ID, &log.MessageID, &log.OrignalState, &log.OrignalStateName, &log.State, &log.StateName, &log.CreationTime, &log.CreationTimeString)
		if err != nil {
			return nil, err
		}
		result = append(result, &log)
	}
	return result, nil
}

func (m *Message) FetchSubscriptions(executor DbExecutor) ([]*Subscription, error) {
	rows, err := executor.QueryScript("FetchSubscriptions", m.ID)
	if err != nil {
		return nil, err
	}
	result := make([]*Subscription, 0)
	for rows.Next() {
		var item Subscription
		err = rows.Scan(&item.ID, &item.MessageID, &item.ReceiverTag, &item.Exchange, &item.RouteKey, &item.StateName, &item.LastMotifyTime, &item.LastMotifyTimeString)
		if err != nil {
			return nil, err
		}
		result = append(result, &item)
	}
	return result, nil
}

func (m *Message) Append(executor DbExecutor) (string, error) {
	if m.ID == "" {
		m.ID = NewOrderedUUID()
	}
	m.State = MessageScheduled
	m.StateName = MessageScheduled.String()
	m.Retry = 0
	m.CreationTime = time.Now().Unix()
	m.CreationTimeString = FormatTime(time.Now())
	_, err := executor.ExecScript("InsertMessage",
		m.ID, m.MessageType, m.Content, m.State, m.StateName,
		m.Retry, m.CreationTime, m.CreationTimeString,
		m.Publisher, m.PublishTime, m.PublishTimeString, m.Env)
	if err != nil {
		return "", err
	}
	return m.ID, nil
}

func (m *Message) Published(executor DbExecutor) error {
	log := &MessageLog{
		ID:                 NewOrderedUUID(),
		MessageID:          m.ID,
		OrignalState:       m.State,
		OrignalStateName:   m.State.String(),
		State:              MessageProcessing,
		StateName:          MessageProcessing.String(),
		CreationTime:       time.Now().Unix(),
		CreationTimeString: FormatTime(time.Now()),
	}
	_, err := executor.ExecScript("PublishedMessage", MessageProcessing, MessageProcessing.String(), time.Now().Unix(), FormatTime(time.Now()), m.ID)
	if err != nil {
		return err
	}
	_, err = log.Append(executor)
	if err != nil {
		return err
	}
	return nil
}

func (m *Message) ChangeState(state MessageState, executor DbExecutor) error {
	log := &MessageLog{
		ID:                 NewOrderedUUID(),
		MessageID:          m.ID,
		OrignalState:       m.State,
		OrignalStateName:   m.State.String(),
		State:              state,
		StateName:          state.String(),
		CreationTime:       time.Now().Unix(),
		CreationTimeString: FormatTime(time.Now()),
	}
	_, err := executor.ExecScript("ChangeMessageState", state, state.String(), m.ID)
	if err != nil {
		return err
	}
	_, err = log.Append(executor)
	if err != nil {
		return err
	}
	return nil
}

func (m *Message) IncreaseRetry(executor DbExecutor) (int32, error) {
	row, err := executor.QueryScriptRow("IncreaseMessageRetry", m.ID)
	if err != nil {
		return 0, err
	}
	var retry int32
	err = row.Scan(&retry)
	if err != nil {
		return 0, err
	}
	return retry, nil
}

func (m *Message) ChangeSubscriptionState(stateName string, receiverTag string, executor DbExecutor) error {
	_, err := executor.ExecScript("ChangeSubscriptionState", stateName, time.Now().Unix(), FormatTime(time.Now()), "NOVALUE", m.ID, receiverTag)
	if err != nil {
		return err
	}
	return nil
}

func (m *Message) AppendDefaultSubscribers(templateName string, executor DbExecutor) error {
	template, err := FindOneTemplate("UNKNOWN", templateName, executor)
	if err != nil {
		return WrapError("AppendDefaultSubscribers", err)
	}
	if template == nil {
		return WrapError("AppendDefaultSubscribers", fmt.Errorf("template %s not found", templateName))
	}
	subs, err := template.Details(executor)
	if err != nil {
		return WrapError("AppendDefaultSubscribers", err)
	}
	for _, sub := range subs {
		s := &Subscription{
			ID:          NewOrderedUUID(),
			MessageID:   m.ID,
			ReceiverTag: sub.ReceiverTag,
			Exchange:    sub.Exchange,
			RouteKey:    sub.RouteKey,
			StateName:   "Scheduled",
		}
		_, err = s.Append(executor)
		if err != nil {
			return WrapError("AppendDefaultSubscribers", err)
		}
	}
	return nil
}

type MessageLog struct {
	ID                 string
	MessageID          string
	OrignalState       MessageState
	OrignalStateName   string
	State              MessageState
	StateName          string
	CreationTime       int64
	CreationTimeString string
}

func (m *MessageLog) Append(executor DbExecutor) (string, error) {
	if m.MessageID == "" {
		return "", WrapError("MessageLog.Append:", errors.New("MessageLog.MessageID was required"))
	}
	if m.ID == "" {
		m.ID = NewOrderedUUID()
	}
	m.CreationTime = time.Now().Unix()
	m.CreationTimeString = FormatTime(time.Now())
	_, err := executor.ExecScript("InsertMessageLog", m.ID, m.MessageID, m.OrignalState, m.OrignalStateName,
		m.State, m.StateName, m.CreationTime, m.CreationTimeString)
	if err != nil {
		return "", err
	}
	return m.ID, nil
}

type Subscription struct {
	ID                   string
	MessageID            string
	ReceiverTag          string
	Exchange             string
	RouteKey             string
	StateName            string
	LastMotifyTime       int64
	LastMotifyTimeString string
}

func (s *Subscription) Append(executor DbExecutor) (string, error) {
	if s.MessageID == "" {
		return "", WrapError("Subscription.Append:", errors.New("Subscription.MessageID was required"))
	}
	if s.ID == "" {
		s.ID = NewOrderedUUID()
	}
	s.StateName = "Scheduled"
	_, err := executor.ExecScript("InsertSubscription", s.ID, s.MessageID, s.ReceiverTag, s.Exchange, s.RouteKey, s.StateName)
	if err != nil {
		return "", err
	}
	return s.ID, nil
}

func (s *Subscription) ChangeState(stateName string, executor DbExecutor) error {
	_, err := executor.ExecScript("ChangeSubscriptionState", stateName, time.Now().Unix(), FormatTime(time.Now()), s.ID, "NOVALUE", "NOVALUE")
	if err != nil {
		return err
	}
	return nil
}

func (s *Subscription) FetchFlows(executor DbExecutor) ([]*Flow, error) {
	rows, err := executor.QueryScript("FetchFlows", s.ID)
	if err != nil {
		return nil, err
	}
	result := make([]*Flow, 0)
	for rows.Next() {
		var item Flow
		err = rows.Scan(&item.ID, &item.SubscriptionID, &item.StateName, &item.Remark, &item.CreationTime, &item.CreationTimeString)
		if err != nil {
			return nil, err
		}
		result = append(result, &item)
	}
	return result, nil
}

type Flow struct {
	ID                 string
	SubscriptionID     string
	StateName          string
	Remark             string
	CreationTime       int64
	CreationTimeString string
}

func (f *Flow) Append(executor DbExecutor) (string, error) {
	if f.SubscriptionID == "" {
		return "", WrapError("Flow.Append:", errors.New("Flow.SubscriptionID was required"))
	}
	if f.ID == "" {
		f.ID = NewOrderedUUID()
	}
	f.CreationTime = time.Now().Unix()
	f.CreationTimeString = FormatTime(time.Now())
	_, err := executor.ExecScript("InsertFlow", f.ID, f.SubscriptionID, f.StateName, f.Remark, f.CreationTime, f.CreationTimeString)
	if err != nil {
		return "", err
	}
	return f.ID, nil
}

type MessageState int16

const (
	_                 MessageState = iota
	MessageScheduled               = MessageState(1)
	MessageProcessing              = MessageState(2)
	MessageSucceeded               = MessageState(3)
	MessageFailed                  = MessageState(4)
	MessageRollback                = MessageState(5)
	MessageUnknown                 = MessageState(99)
)

func TryParseMessageState(s string) (MessageState, error) {
	state := ParseMessageState(s)
	if state == -1 {
		return MessageUnknown, fmt.Errorf("messageState '%s' unrecognizable", s)
	}
	return state, nil
}

func ParseMessageState(str string) MessageState {
	if str == "Scheduled" {
		return MessageScheduled
	} else if str == "Processing" {
		return MessageProcessing
	} else if str == "Succeeded" {
		return MessageSucceeded
	} else if str == "Failed" {
		return MessageFailed
	} else if str == "Unknown" {
		return MessageUnknown
	} else if str == "Rollback" {
		return MessageRollback
	}
	return MessageUnknown
}

func (state MessageState) String() string {
	switch state {
	case MessageScheduled:
		return "Scheduled"
	case MessageProcessing:
		return "Processing"
	case MessageSucceeded:
		return "Succeeded"
	case MessageFailed:
		return "Failed"
	case MessageRollback:
		return "Rollback"
	}
	return "Unknown"
}

func FindOneMessage(ID string, lock bool, executor DbExecutor) (*Message, error) {
	scriptName := "FindOneMessage"
	if lock {
		scriptName = "FindOneLockedMessage"
	}

	row, err := executor.QueryScriptRow(scriptName, ID)
	if err != nil {
		return nil, err
	}
	var m Message
	err = row.Scan(&m.ID, &m.MessageType, &m.Content, &m.State,
		&m.StateName, &m.Retry, &m.CreationTime, &m.CreationTimeString,
		&m.Publisher, &m.PublishTime, &m.PublishTimeString, &m.Env)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func FindProcessingMessageIDs(executor DbExecutor) ([]string, error) {
	rows, err := executor.QueryScript("FindProcessingMessage")
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for rows.Next() {
		var ID string
		err = rows.Scan(&ID)
		result = append(result, ID)
	}
	return result, nil
}

func AppendMessage(payload *Payload, extsHandler func(*ExtensionsEventArgs, DbExecutor) error, executor DbExecutor) (*Message, error) {
	msg := NewMessage()
	msg.MessageType = payload.MessageType
	msg.Content = payload.Content

	if v, ok := payload.Headers["x-matcha-client"]; ok {
		msg.Publisher = v.(string)
	}

	if v, ok := payload.Headers["x-matcha-time"]; ok {
		u, err := strconv.ParseInt(v.(string), 10, 64)
		if err != nil {
			return nil, err
		}
		msg.PublishTime = u
		msg.PublishTimeString = FormatTime(time.Unix(u, 0))
	}

	messageID, err := msg.Append(executor)
	if err != nil {
		return nil, err
	}

	log := &MessageLog{
		MessageID:        messageID,
		OrignalState:     MessageUnknown,
		OrignalStateName: MessageUnknown.String(),
		State:            MessageScheduled,
		StateName:        MessageScheduled.String(),
	}
	_, err = log.Append(executor)
	if err != nil {
		return nil, err
	}

	for _, subPayload := range payload.Subscriptions {
		sub := &Subscription{
			MessageID:   messageID,
			ReceiverTag: subPayload.Tag,
			Exchange:    subPayload.Exchange,
			RouteKey:    subPayload.RouteKey,
			StateName:   "Scheduled",
		}
		subID, err := sub.Append(executor)
		if err != nil {
			return nil, err
		}

		flow := &Flow{
			SubscriptionID: subID,
			StateName:      "Scheduled",
			Remark:         "",
		}
		_, err = flow.Append(executor)
		if err != nil {
			return nil, err
		}
	}

	if extsHandler != nil {
		err = extsHandler(&ExtensionsEventArgs{
			Extensions: payload.Extensions,
			MessageID:  messageID,
		}, executor)
		if err != nil {
			return nil, err
		}
	}

	return msg, nil
}

func FindSubscription(id string, messageID string, receiverTag string, executor DbExecutor) (*Subscription, error) {
	row, err := executor.QueryScriptRow("FindSubscription", id, messageID, receiverTag)
	if err != nil {
		return nil, err
	}
	var m Subscription
	err = row.Scan(&m.ID, &m.MessageID, &m.ReceiverTag, &m.Exchange, &m.RouteKey, &m.StateName)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func FineOneLockSubscription(id string, messageID string, receiverTag string, executor DbExecutor) (*Subscription, error) {
	rows, err := executor.QueryScriptRow("FindOneLockedSubscription", id, messageID, receiverTag)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	var m Subscription
	err = rows.Scan(&m.ID, &m.MessageID, &m.ReceiverTag, &m.Exchange, &m.RouteKey, &m.StateName)
	if err != nil {
		return nil, nil
	}
	return &m, nil
}

type failedMessage struct {
	MessageID   string
	MessageType string
}

func findFailedMessage(executor DbExecutor) (*failedMessage, error) {
	rows, err := executor.QueryScript("FindFailedMessage")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var m failedMessage
		err = rows.Scan(&m.MessageID, &m.MessageType)
		if err != nil {
			return nil, err
		}
		return &m, nil
	}
	return nil, nil
}
