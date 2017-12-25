package api

import (
	"fmt"
	"strings"

	"github.com/standardcore/Matcha/essentials"
	. "gopkg.in/ahmetb/go-linq.v3"
)

type Job struct {
	MessageID    string `json:"id"`
	State        string `json:"state"`
	CreationTime string `json:"creation_time"`
	Publisher    string `json:"publisher"`
	PublishTime  string `json:"publish_time"`
	Expression   string `json:"expression"`
	Kind         string `json:"kind"`
	DelaySeconds int32  `json:"delay"`
	SubID        string `json:"sub_id"`
	Exchange     string `json:"exchange"`
	RoutingKey   string `json:"key"`
	Stage        string `json:"stage"`
	StageTime    string `json:"stage_time"`

	Flows []interface{} `json:"flows"`
}

type hashableJob struct {
	MessageID    string
	State        string
	CreationTime string
	Publisher    string
	PublishTime  string
	Expression   string
	Kind         string
	DelaySeconds int32
	SubID        string
	Exchange     string
	RoutingKey   string
	Stage        string
	StageTime    string
}

type JobFlow struct {
	ID           string  `json:"id"`
	State        string  `json:"state"`
	Remark       *string `json:"remark"`
	CreationTime string  `json:"creation_time"`
}

type JobParameters struct {
	PublishTimeStart *int64  `json:"start"`
	PublishTimeEnd   *int64  `json:"end"`
	MessageState     *string `json:"state"`
	Pager            bool    `json:"pager"`
	Skip             int32   `json:"skip"`
	Take             int32   `json:"take"`
}

type listJobQueryModel struct {
	ID                 string
	StateName          string
	CreationTimeString string
	Publisher          *string
	PublishTime        *string
	Expression         string
	KindName           string
	DelaySeconds       int32
	SubID              string
	Exchange           string
	RouteKey           string
	Stage              string
	StageTime          string
	FlowID             string
	FlowState          string
	Remark             *string
	FlowTime           string
}

func ListJobMessages(p *JobParameters, executor essentials.DbExecutor, sess *essentials.Session) ([]interface{}, error) {
	script, err := sess.Script("ListJobs")
	if err != nil {
		return nil, err
	}
	sql, err := script.Compile()
	if err != nil {
		return nil, err
	}
	whereClauses := make([]string, 0)
	parameters := make([]interface{}, 0)
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
	entities := make([]*listJobQueryModel, 0)
	for rows.Next() {
		var entity listJobQueryModel
		err = rows.Scan(&entity.ID, &entity.StateName, &entity.CreationTimeString,
			&entity.Publisher, &entity.PublishTime, &entity.Expression, &entity.KindName,
			&entity.DelaySeconds, &entity.SubID, &entity.Exchange, &entity.RouteKey,
			&entity.Stage, &entity.StageTime, &entity.FlowID, &entity.FlowState,
			&entity.Remark, &entity.FlowTime)
		if err != nil {
			return nil, err
		}
		entities = append(entities, &entity)
	}

	if len(entities) == 0 {
		return nil, nil
	}

	result := From(entities).GroupBy(func(msg interface{}) interface{} {
		job := hashableJob{
			MessageID:    msg.(*listJobQueryModel).ID,
			State:        msg.(*listJobQueryModel).StateName,
			CreationTime: msg.(*listJobQueryModel).CreationTimeString,
			Expression:   msg.(*listJobQueryModel).Expression,
			Kind:         msg.(*listJobQueryModel).KindName,
			DelaySeconds: msg.(*listJobQueryModel).DelaySeconds,
			SubID:        msg.(*listJobQueryModel).SubID,
			Exchange:     msg.(*listJobQueryModel).Exchange,
			RoutingKey:   msg.(*listJobQueryModel).RouteKey,
			Stage:        msg.(*listJobQueryModel).Stage,
			StageTime:    msg.(*listJobQueryModel).StageTime,
		}
		if msg.(*listJobQueryModel).Publisher != nil {
			job.Publisher = *msg.(*listJobQueryModel).Publisher
		}
		if msg.(*listJobQueryModel).PublishTime != nil {
			job.PublishTime = *msg.(*listJobQueryModel).PublishTime
		}
		return job
	}, func(v1 interface{}) interface{} {
		return v1
	}).Select(func(g1 interface{}) interface{} {
		return &Job{
			MessageID:    g1.(Group).Key.(hashableJob).MessageID,
			State:        g1.(Group).Key.(hashableJob).State,
			CreationTime: g1.(Group).Key.(hashableJob).CreationTime,
			Publisher:    g1.(Group).Key.(hashableJob).Publisher,
			PublishTime:  g1.(Group).Key.(hashableJob).PublishTime,
			Expression:   g1.(Group).Key.(hashableJob).Expression,
			Kind:         g1.(Group).Key.(hashableJob).Kind,
			DelaySeconds: g1.(Group).Key.(hashableJob).DelaySeconds,
			SubID:        g1.(Group).Key.(hashableJob).SubID,
			Exchange:     g1.(Group).Key.(hashableJob).Exchange,
			RoutingKey:   g1.(Group).Key.(hashableJob).RoutingKey,
			Stage:        g1.(Group).Key.(hashableJob).Stage,
			StageTime:    g1.(Group).Key.(hashableJob).StageTime,
			Flows: From(g1.(Group).Group).Select(func(flow interface{}) interface{} {
				return &JobFlow{
					ID:           flow.(*listJobQueryModel).ID,
					State:        flow.(*listJobQueryModel).FlowState,
					Remark:       flow.(*listJobQueryModel).Remark,
					CreationTime: flow.(*listJobQueryModel).FlowTime,
				}
			}).DistinctBy(func(distinctFlow interface{}) interface{} {
				return distinctFlow.(*JobFlow).ID
			}).Results(),
		}
	}).DistinctBy(func(distinctJob interface{}) interface{} {
		return distinctJob.(*Job).MessageID
	}).Results()
	return result, nil
}
