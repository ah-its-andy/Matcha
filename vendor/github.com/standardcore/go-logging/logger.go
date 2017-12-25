package logging

type Logger interface {
	AddProvider(provider LoggerProvider)
	TryAddProvider(provider LoggerProvider, err error) error
	Print(v ...interface{})
	Printf(format string, args ...interface{})
	Println(v ...interface{})
	Debug(v ...interface{})
	Debugf(format string, args ...interface{})
	Debugln(v ...interface{})
	Info(v ...interface{})
	Infof(format string, args ...interface{})
	Infoln(v ...interface{})
	Warn(v ...interface{})
	Warnf(format string, args ...interface{})
	Warnln(v ...interface{})
	Error(v ...interface{})
	Errorf(format string, args ...interface{})
	Errorln(v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, args ...interface{})
	Fatalln(v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, args ...interface{})
	Panicln(v ...interface{})
	Sync() error
}
