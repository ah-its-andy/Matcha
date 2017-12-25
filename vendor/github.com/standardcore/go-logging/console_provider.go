package logging

import (
	"fmt"

	"github.com/xcltapestry/xclpkg/clcolor"
)

type ConsoleProvider struct {
}

func NewConsoleProvider() (LoggerProvider, error) {
	return &ConsoleProvider{}, nil
}

func (provider *ConsoleProvider) Log(level Level, msg string) {
	switch level {
	case LevelDebug:
		fmt.Printf("[%s:] %s", clcolor.White(level.String()), msg)
	case LevelError:
		fmt.Printf("[%s:] %s", clcolor.Red(level.String()), msg)
	case LevelFatal:
		fmt.Printf("[%s:] %s", clcolor.Red(level.String()), msg)
	case LevelInfo:
		fmt.Printf("[%s:] %s", clcolor.Green(level.String()), msg)
	case LevelPanic:
		fmt.Printf("[%s:] %s", clcolor.Red(level.String()), msg)
	case LevelWarn:
		fmt.Printf("[%s:] %s", clcolor.Blue(level.String()), msg)
	}
}

func (provider *ConsoleProvider) Sync() error {
	return nil
}
