package log

import (
	"fmt"
	"runtime"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	log *zap.Logger
}

func CallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(strings.Join([]string{caller.TrimmedPath(), runtime.FuncForPC(caller.PC).Name()}, ":"))
}

func New(debugLevel bool) (*Logger, error) {
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.EncodeCaller = CallerEncoder

	if debugLevel {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	_log, err := config.Build()
	if err != nil {
		return nil, err
	}

	return &Logger{log: _log}, nil
}

func NewLogstash(debugLevel bool, host string, port int) (*Logger, error) {
	return NewLogstashWithTimeout(debugLevel, host, port, 10)
}

func NewLogstashWithTimeout(debugLevel bool, host string, port int, timeout int) (*Logger, error) {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeCaller = CallerEncoder
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncodeDuration = zapcore.SecondsDurationEncoder
	cfg.EncodeLevel = zapcore.LowercaseLevelEncoder

	enc := zapcore.NewJSONEncoder(cfg)
	sink, err := NewUDPSyncer(host, port, timeout)
	if err != nil {
		return nil, err
	}

	var atom zap.AtomicLevel
	if debugLevel {
		atom = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else {
		atom = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	_log := zap.New(zapcore.NewCore(enc, sink, atom), zap.AddCaller(), zap.AddStacktrace(atom))
	return &Logger{log: _log}, nil
}

func (l *Logger) Print(v ...interface{}) {
	l.log.Info(fmt.Sprint(v))
}

func (l *Logger) Printf(format string, args ...interface{}) {
	l.log.Info(fmt.Sprintf(format, args...))
}

func (l *Logger) Println(v ...interface{}) {
	l.log.Info(fmt.Sprintln(v))
}

func (l *Logger) Debug(v ...interface{}) {
	l.log.Debug(fmt.Sprint(v))
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log.Debug(fmt.Sprintf(format, args...))
}

func (l *Logger) Debugln(v ...interface{}) {
	l.log.Debug(fmt.Sprintln(v))
}

func (l *Logger) Info(v ...interface{}) {
	l.log.Info(fmt.Sprint(v))
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.log.Info(fmt.Sprintf(format, args...))
}

func (l *Logger) Infoln(v ...interface{}) {
	l.log.Info(fmt.Sprintln(v))
}

func (l *Logger) Warn(v ...interface{}) {
	l.log.Warn(fmt.Sprint(v))
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log.Warn(fmt.Sprintf(format, args...))
}

func (l *Logger) Warnln(v ...interface{}) {
	l.log.Warn(fmt.Sprintln(v))
}

func (l *Logger) Error(v ...interface{}) {
	l.log.Error(fmt.Sprint(v))
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log.Error(fmt.Sprintf(format, args...))
}

func (l *Logger) Errorln(v ...interface{}) {
	l.log.Error(fmt.Sprintln(v))
}

func (l *Logger) Fatal(v ...interface{}) {
	l.log.Fatal(fmt.Sprint(v))
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.log.Fatal(fmt.Sprintf(format, args...))
}

func (l *Logger) Fatalln(v ...interface{}) {
	l.log.Fatal(fmt.Sprintln(v))
}

func (l *Logger) Panic(v ...interface{}) {
	l.log.Panic(fmt.Sprint(v))
}

func (l *Logger) Panicf(format string, args ...interface{}) {
	l.log.Panic(fmt.Sprintf(format, args...))
}

func (l *Logger) Panicln(v ...interface{}) {
	l.log.Panic(fmt.Sprintln(v))
}

func (l *Logger) With(key string, value interface{}) *Logger {
	return &Logger{l.log.With(zap.Any(key, value))}
}

func (l *Logger) Sync() error {
	return l.log.Sync()
}
