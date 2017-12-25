package essentials

import (
	"github.com/FeiniuBus/log"
	"github.com/standardcore/go-logging"
)

type SyslogProvider struct {
	logger *log.Logger
}

func NewSyslogProvider(debugLevel bool, appName string) (logging.LoggerProvider, error) {
	syslog, err := log.NewSyslog(debugLevel, appName)
	return &SyslogProvider{
		logger: syslog,
	}, err
}

func (provider *SyslogProvider) Log(level logging.Level, msg string) {
	switch level {
	case logging.LevelDebug:
		provider.logger.Debug(msg)
		break
	case logging.LevelError:
		provider.logger.Error(msg)
		break
	case logging.LevelFatal:
		provider.logger.Fatal(msg)
		break
	case logging.LevelInfo:
		provider.logger.Info(msg)
		break
	case logging.LevelPanic:
		provider.logger.Panic(msg)
		break
	case logging.LevelWarn:
		provider.logger.Warn(msg)
		break
	}
}

func (provider *SyslogProvider) Sync() error {
	return provider.logger.Sync()
}

type LogStashProvider struct {
	logger *log.Logger
}

func NewLogstashProvider(debugLevel bool, host string, port int) (logging.LoggerProvider, error) {
	stash, err := log.NewLogstashWithTimeout(debugLevel, host, port, 10)
	defer stash.Sync()
	return &LogStashProvider{
		logger: stash,
	}, err
}

func (provider *LogStashProvider) Sync() error {
	return provider.logger.Sync()
}

func (provider *LogStashProvider) Log(level logging.Level, msg string) {
	switch level {
	case logging.LevelDebug:
		provider.logger.Debug(msg)
	case logging.LevelError:
		provider.logger.Error(msg)
	case logging.LevelFatal:
		provider.logger.Fatal(msg)
	case logging.LevelInfo:
		provider.logger.Info(msg)
	case logging.LevelPanic:
		provider.logger.Panic(msg)
	case logging.LevelWarn:
		provider.logger.Warn(msg)
	}
}
