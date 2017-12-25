// +build !windows,!nacl,!plan9

package log

import (
	"log/syslog"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewSyslogSyncer() (*SyslogSyncer, error) {
	s := &SyslogSyncer{}
	return s, s.connect()
}

func NewSyslog(debugLevel bool, app string) (*Logger, error) {
	enc := NewSyslogEncoder(SyslogEncoderConfig{
		EncoderConfig: zapcore.EncoderConfig{
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   CallerEncoder,
		},

		Facility: syslog.LOG_LOCAL0,
		Hostname: "localhost",
		PID:      os.Getpid(),
		App:      app,
	})

	sink, err := NewSyslogSyncer()
	if err != nil {
		return nil, err
	}

	var atom zap.AtomicLevel
	if debugLevel {
		atom = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else {
		atom = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	_log := zap.New(zapcore.NewCore(enc, zapcore.Lock(sink), atom))
	return &Logger{log: _log}, nil
}
