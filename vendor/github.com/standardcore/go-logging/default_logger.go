package logging

import "fmt"

type DefaultLogger struct {
	providers []LoggerProvider
}

func NewLogger() Logger {
	return &DefaultLogger{
		providers: make([]LoggerProvider, 0),
	}
}

func (logger *DefaultLogger) AddProvider(provider LoggerProvider) {
	logger.providers = append(logger.providers, provider)
}

func (logger *DefaultLogger) TryAddProvider(provider LoggerProvider, err error) error {
	if err != nil {
		return err
	}
	logger.AddProvider(provider)
	return nil
}

func (logger *DefaultLogger) Print(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelInfo, fmt.Sprint(v))
	}
}

func (logger *DefaultLogger) Printf(format string, args ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelInfo, fmt.Sprintf(format, args))
	}
}

func (logger *DefaultLogger) Println(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelInfo, fmt.Sprintln(v))
	}
}

func (logger *DefaultLogger) Debug(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelDebug, fmt.Sprint(v))
	}
}

func (logger *DefaultLogger) Debugf(format string, args ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelDebug, fmt.Sprintf(format, args))
	}
}

func (logger *DefaultLogger) Debugln(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelDebug, fmt.Sprintln(v))
	}
}

func (logger *DefaultLogger) Info(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelInfo, fmt.Sprint(v))
	}
}

func (logger *DefaultLogger) Infof(format string, args ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelInfo, fmt.Sprintf(format, args))
	}
}

func (logger *DefaultLogger) Infoln(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelInfo, fmt.Sprintln(v))
	}
}

func (logger *DefaultLogger) Warn(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelWarn, fmt.Sprint(v))
	}
}

func (logger *DefaultLogger) Warnf(format string, args ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelWarn, fmt.Sprintf(format, args))
	}
}

func (logger *DefaultLogger) Warnln(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelWarn, fmt.Sprintln(v))
	}
}

func (logger *DefaultLogger) Error(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelError, fmt.Sprint(v))
	}
}

func (logger *DefaultLogger) Errorf(format string, args ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelError, fmt.Sprintf(format, args))
	}
}

func (logger *DefaultLogger) Errorln(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelError, fmt.Sprintln(v))
	}
}

func (logger *DefaultLogger) Fatal(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelFatal, fmt.Sprint(v))
	}
}

func (logger *DefaultLogger) Fatalf(format string, args ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelFatal, fmt.Sprintf(format, args))
	}
}

func (logger *DefaultLogger) Fatalln(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelFatal, fmt.Sprintln(v))
	}
}

func (logger *DefaultLogger) Panic(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelPanic, fmt.Sprint(v))
	}
}

func (logger *DefaultLogger) Panicf(format string, args ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelPanic, fmt.Sprintf(format, args))
	}
}

func (logger *DefaultLogger) Panicln(v ...interface{}) {
	for _, p := range logger.providers {
		p.Log(LevelPanic, fmt.Sprintln(v))
	}
}

func (logger *DefaultLogger) Sync() error {
	for _, p := range logger.providers {
		err := p.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}
