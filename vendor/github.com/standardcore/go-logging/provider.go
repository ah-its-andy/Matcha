package logging

type LoggerProvider interface {
	Log(level Level, msg string)
	Sync() error
}
