package essentials

import (
	"database/sql"
	"encoding/json"
)

func ExecuteChangeState(content []byte, sess *Session) error {
	// content, err := ioutil.ReadAll(m.Body)
	// if err != nil {
	// 	return err
	// }
	var payload ChangeStatePayload
	err := json.Unmarshal(content, &payload)
	if err != nil {
		return err
	}
	conn, err := sess.CreateConnectionFactory().Database()
	if err != nil {
		return err
	}
	defer conn.Close()
	transact, err := conn.BeginTx(sql.LevelReadCommitted)
	if err != nil {
		return err
	}
	sub, err := FineOneLockSubscription("UNKNOWN", payload.MessageID, payload.ClientTag, transact)
	if err != nil {
		return err
	}
	if sub == nil || sub.ID == "" {
		sub = &Subscription{
			ID:          NewOrderedUUID(),
			MessageID:   payload.MessageID,
			ReceiverTag: payload.ClientTag,
			Exchange:    "UNKNOWN",
			RouteKey:    "UNKNOWN",
			StateName:   payload.NewState,
		}
		if v, ok := payload.Extensions["x-matcha-exchange"]; ok {
			sub.Exchange = v
		}
		if v, ok := payload.Extensions["x-matcha-exchange"]; ok {
			sub.RouteKey = v
		}
		_, err = sub.Append(transact)
		if err != nil {
			return err
		}
	} else {
		err = sub.ChangeState(payload.NewState, transact)
		if err != nil {
			return err
		}
	}

	flow := &Flow{
		ID:             NewOrderedUUID(),
		SubscriptionID: sub.ID,
		StateName:      payload.NewState,
		Remark:         payload.Remark,
	}

	_, err = flow.Append(transact)
	if err != nil {
		return err
	}

	err = transact.Commit()
	if err != nil {
		return err
	}

	return nil
}
