package logging

type Level int

const (
	_ Level = iota
	LevelInfo
	LevelDebug
	LevelWarn
	LevelError
	LevelFatal
	LevelPanic
)

func (level Level) String() string {
	switch level {
	case LevelDebug:
		return "debug"
	case LevelError:
		return "error"
	case LevelFatal:
		return "fatal"
	case LevelInfo:
		return "info"
	case LevelPanic:
		return "panic"
	case LevelWarn:
		return "warn"
	}
	return "Unknown"
}
