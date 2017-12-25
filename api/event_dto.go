package api

import (
	"fmt"
	"strings"

	"github.com/standardcore/Matcha/essentials"
	. "gopkg.in/ahmetb/go-linq.v3"
)

type EventMessageDto struct {
	ID           string        `json:"message_id"`
	Exchange     string        `json:"exchange"`
	RoutingKey   string        `json:"key"`
	TargetClient string        `json:"target_client"`
	State        string        `json:"state"`
	Publisher    string        `json:"publisher"`
	PublishTime  string        `json:"publish_time"`
	Subs         []interface{} `json:"subs"`
	Logs         []interface{} `json:"logs"`
}

type hashableEventMessageDto struct {
	ID           string
	Exchange     string
	RoutingKey   string
	TargetClient string
	State        string
	Publisher    string
	PublishTime  string
}

type EventSubDto struct {
	ID             string        `json:"sub_id"`
	ReceiverTag    string        `json:"tag"`
	StateName      string        `json:"state"`
	LastMotifyTime string        `json:"last_motify_time"`
	Flows          []interface{} `json:"flows"`
}

type hashableEventSubDto struct {
	ID             string
	ReceiverTag    string
	StateName      string
	LastMotifyTime string
}

type EventFlowDto struct {
	ID           string  `json:"flow_id"`
	StateName    string  `json:"state"`
	Remark       *string `json:"remark"`
	CreationTime string  `json:"creation_time"`
}

type EventLogDto struct {
	ID           string `json:"log_id"`
	OrignalState string `json:"orignal"`
	State        string `json:"current"`
	CreationTime string `json:"creation_time"`
}

type ListEventMessagesParameters struct {
	PublishTimeStart *int64  `json:"start"`
	PublishTimeEnd   *int64  `json:"end"`
	MessageState     *string `json:"state"`
	Pager            bool    `json:"pager"`
	Skip             int32   `json:"skip"`
	Take             int32   `json:"take"`
}

type listEventQueryModel struct {
	MessageID         string
	MessageState      string
	Publisher         *string
	PublishTimeString *string
	RouteKey          string
	Queue             *string
	Exchange          string
	LogID             string
	LogOrignal        string
	LogCurrent        string
	LogTime           string
	SubID             string
	ReceiverTag       string
	SubState          string
	SubTime           string
	FlowID            string
	FlowState         string
	Remark            *string
	FlowTime          string
}

func (m *listEventQueryModel) makeEventMessageDto() hashableEventMessageDto {
	result := hashableEventMessageDto{
		ID:         m.MessageID,
		Exchange:   m.Exchange,
		RoutingKey: m.RouteKey,
		State:      m.MessageState,
	}
	if m.Queue != nil {
		result.TargetClient = *m.Queue
	}
	if m.Publisher != nil {
		result.Publisher = *m.Publisher
	}
	if m.PublishTimeString != nil {
		result.PublishTime = *m.PublishTimeString
	}
	return result
}

func (m *listEventQueryModel) makeEventSubDto() hashableEventSubDto {
	result := hashableEventSubDto{
		ID:             m.SubID,
		ReceiverTag:    m.ReceiverTag,
		StateName:      m.SubState,
		LastMotifyTime: m.SubTime,
	}
	return result
}

func ListEventMessages(p *ListEventMessagesParameters, executor essentials.DbExecutor, sess *essentials.Session) ([]interface{}, error) {
	script, err := sess.Script("ListEvents")
	if err != nil {
		return nil, err
	}
	sql, err := script.Compile()
	if err != nil {
		return nil, err
	}
	parameters := make([]interface{}, 0)
	whereClauses := make([]string, 0)
	index := 0
	if p.PublishTimeStart != nil && p.PublishTimeEnd != nil {
		index++
		whereClauses = append(whereClauses, fmt.Sprintf("msg.\"PublishTime\">=$%d", index))
		parameters = append(parameters, (*p.PublishTimeStart))
		index++
		whereClauses = append(whereClauses, fmt.Sprintf("msg.\"PublishTime\"<=$%d", index))
		parameters = append(parameters, (*p.PublishTimeEnd))
	}
	if p.MessageState != nil {
		state := essentials.ParseMessageState(*p.MessageState)
		index++
		whereClauses = append(whereClauses, fmt.Sprintf("msg.\"State\"=$%d", index))
		parameters = append(parameters, (state))
	}
	if len(whereClauses) > 1 {
		sql += "AND" + strings.Join(whereClauses, " AND ")
	} else if len(whereClauses) == 1 {
		sql += "AND" + whereClauses[0]
	}

	if p.Pager {
		sql += fmt.Sprintf(" LIMIT %d OFFSET %d ", p.Take, p.Skip)
	}

	rows, err := executor.Query(sql, parameters...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entities := make([]*listEventQueryModel, 0)
	for rows.Next() {
		var entity listEventQueryModel
		err = rows.Scan(&entity.MessageID, &entity.MessageState, &entity.Publisher,
			&entity.PublishTimeString, &entity.RouteKey, &entity.Queue, &entity.Exchange,
			&entity.LogID, &entity.LogOrignal, &entity.LogCurrent, &entity.LogTime,
			&entity.SubID, &entity.ReceiverTag, &entity.SubState, &entity.SubTime,
			&entity.FlowID, &entity.FlowState, &entity.Remark, &entity.FlowTime)
		if err != nil {
			return nil, err
		}
		entities = append(entities, &entity)
	}

	if len(entities) == 0 {
		return nil, nil
	}

	results :=
		From(entities).GroupBy(func(msg interface{}) interface{} {
			return msg.(*listEventQueryModel).makeEventMessageDto()
		}, func(v1 interface{}) interface{} {
			return v1
		}).Select(func(g1 interface{}) interface{} {
			return &EventMessageDto{
				ID:           g1.(Group).Key.(hashableEventMessageDto).ID,
				Exchange:     g1.(Group).Key.(hashableEventMessageDto).Exchange,
				RoutingKey:   g1.(Group).Key.(hashableEventMessageDto).RoutingKey,
				TargetClient: g1.(Group).Key.(hashableEventMessageDto).TargetClient,
				State:        g1.(Group).Key.(hashableEventMessageDto).State,
				Publisher:    g1.(Group).Key.(hashableEventMessageDto).Publisher,
				PublishTime:  g1.(Group).Key.(hashableEventMessageDto).PublishTime,
				Subs: From(g1.(Group).Group).GroupBy(func(sub interface{}) interface{} {
					return sub.(*listEventQueryModel).makeEventSubDto()
				}, func(v2 interface{}) interface{} {
					return v2
				}).Select(func(g2 interface{}) interface{} {
					return &EventSubDto{
						ID:             g2.(Group).Key.(hashableEventSubDto).ID,
						ReceiverTag:    g2.(Group).Key.(hashableEventSubDto).ReceiverTag,
						StateName:      g2.(Group).Key.(hashableEventSubDto).StateName,
						LastMotifyTime: g2.(Group).Key.(hashableEventSubDto).LastMotifyTime,
						Flows: From(g2.(Group).Group).Select(func(flow interface{}) interface{} {
							return &EventFlowDto{
								ID:           flow.(*listEventQueryModel).FlowID,
								StateName:    flow.(*listEventQueryModel).FlowState,
								Remark:       flow.(*listEventQueryModel).Remark,
								CreationTime: flow.(*listEventQueryModel).FlowTime,
							}
						}).DistinctBy(func(distinctFlow interface{}) interface{} {
							return distinctFlow.(*EventFlowDto).ID
						}).Results(),
					}
				}).DistinctBy(func(distinctSub interface{}) interface{} {
					return distinctSub.(*EventSubDto).ID
				}).Results(),
				Logs: From(g1.(Group).Group).Select(func(log interface{}) interface{} {
					return &EventLogDto{
						ID:           log.(*listEventQueryModel).LogID,
						OrignalState: log.(*listEventQueryModel).LogOrignal,
						State:        log.(*listEventQueryModel).LogCurrent,
						CreationTime: log.(*listEventQueryModel).LogTime,
					}
				}).DistinctBy(func(distinctLog interface{}) interface{} {
					return distinctLog.(*EventLogDto).ID
				}).Results(),
			}
		}).DistinctBy(func(distinctMsg interface{}) interface{} {
			return distinctMsg.(*EventMessageDto).ID
		}).Results()
	return results, nil
}
