package logger

import (
	"os"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

var logger log.Logger
var lock = &sync.Mutex{}

func GetLogger() log.Logger {
	lock.Lock()
	defer lock.Unlock()

	// create one shared logger
	if logger == nil {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
		logger = log.With(logger, "ts", log.DefaultTimestampUTC) // add timestamp
		// logger = log.With(logger, "caller", log.DefaultCaller)   // add caller=example.go:33

		envLogLevel := os.Getenv("LOG_LEVEL")

		logLevel := level.AllowInfo()
		switch envLogLevel {
		case "debug":
			logLevel = level.AllowDebug()
		case "info":
			logLevel = level.AllowInfo()
		case "warn":
			logLevel = level.AllowWarn()
		case "error":
			logLevel = level.AllowError()
		default:
			logLevel = level.AllowInfo()
		}

		logger = level.NewFilter(logger, logLevel)
	}

	return logger
}

// func Log(msg string, keyvals ...interface{}) {
// 	GetLogger().Log("msg", msg, keyvals)
// }

func Debug(msg string, keyvals ...interface{}) {
	keyvals = prependMsg(msg, keyvals...)
	level.Debug(GetLogger()).Log(keyvals...)
}

func Info(msg string, keyvals ...interface{}) {
	keyvals = prependMsg(msg, keyvals...)
	level.Info(GetLogger()).Log(keyvals...)
}

func Warn(msg string, keyvals ...interface{}) {
	keyvals = prependMsg(msg, keyvals...)
	level.Warn(GetLogger()).Log(keyvals...)
}

func Error(msg string, keyvals ...interface{}) {
	keyvals = prependMsg(msg, keyvals...)
	level.Error(GetLogger()).Log(keyvals...)
}

func prependMsg(msg string, keyvals ...interface{}) []interface{} {
	newKeyVals := []interface{}{
		"msg", msg,
	}
	for _, keyval := range keyvals {
		newKeyVals = append(newKeyVals, keyval)
	}
	return newKeyVals
}
