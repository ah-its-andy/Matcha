package backgroundjob

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/standardcore/Matcha/essentials"
)

type Job struct {
	ID           string
	MessageID    string
	Expression   string
	Kind         JobKind
	KindName     string
	DelaySeconds int32
}

func (job *Job) Append(executor essentials.DbExecutor) (string, error) {
	if job.MessageID == "" {
		return "", essentials.WrapError("Job.Append:", errors.New("Job.MessageID was required"))
	}
	if job.ID == "" {
		job.ID = essentials.NewOrderedUUID()
	}
	_, err := executor.ExecScript("InsertBackgroundJob", job.ID, job.MessageID, job.Expression, job.Kind, job.KindName, job.DelaySeconds)
	if err != nil {
		return "", err
	}
	return job.ID, nil
}

func HandlePayloadExtension(e *essentials.ExtensionsEventArgs, executor essentials.DbExecutor) error {
	expression, ok := e.Extensions["expression"]
	if ok == false {
		return fmt.Errorf("extension item '%s' was required", "expression")
	}
	delay, ok := e.Extensions["delay"]
	if ok == false {
		return fmt.Errorf("extension item '%s' was required", "delay")
	}
	delaySeconds, err := strconv.ParseInt(delay, 10, 32)
	if err != nil {
		return err
	}
	job := &Job{
		MessageID:    e.MessageID,
		Expression:   expression,
		DelaySeconds: int32(delaySeconds),
		Kind:         BackgroundJob,
		KindName:     BackgroundJob.String(),
	}
	if delay != "0" {
		job.Kind = DelayJob
		job.KindName = DelayJob.String()
	}
	_, err = job.Append(executor)
	if err != nil {
		return err
	}
	return nil
}

type AtJobDescriptor struct {
	MessageId    string
	DelaySeconds int64
}
