package backgroundjob

type JobKind int16

const (
	_ JobKind = iota
	BackgroundJob
	DelayJob
)

func ParseJobKind(kind string) JobKind {
	if kind == "BackgroundJob" {
		return BackgroundJob
	}
	return DelayJob
}

func (kind JobKind) String() string {
	if kind == BackgroundJob {
		return "BackgroundJob"
	}
	return "DelayJob"
}
