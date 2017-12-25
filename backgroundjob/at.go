package backgroundjob

import (
	"github.com/standardcore/Matcha/essentials"
)

var scheduler *essentials.Scheduler

func GetScheduler() *essentials.Scheduler {
	if scheduler == nil {
		scheduler = essentials.NewScheduler()
	}
	return scheduler
}

func RebuildScheduler(sess *essentials.Session) error {
	conn, err := sess.CreateConnectionFactory().Database()
	if err != nil {
		return essentials.WrapError("RebuildScheduler", err)
	}

	messageIDs, err := essentials.FindProcessingMessageIDs(conn)
	if err != nil {
		return err
	}
	for _, id := range messageIDs {
		job := sess.CreateAtJobFactory().New(id)
		GetScheduler().AddJob("backgroundJob", job)
	}
	return nil
}
