package errors

import (
	"agent/logger"
	"runtime"
)

// global channel for reported errors and errors from panics
// we report errors in the agent up to the api to proactively
// identify agent issues
var ErrorsChannel = make(chan *ErrorReport)

type ErrorReport struct {
	Error      error
	Panic      bool
	StackTrace string
}

func DeferRecover() {
	if panicError := recover(); panicError != nil {
		err := extractErrorFromPanicRecover(panicError)

		if err != nil {
			ReportPanic(err)
		}
	}
}

// return error to the caller if they want to do additional logging / handling
func DeferRecoverWithCallback(f func(error)) {
	if panicError := recover(); panicError != nil {
		err := extractErrorFromPanicRecover(panicError)

		if err != nil {
			ReportPanic(err)
			f(err)
		}
	}
}

func Report(err error) {
	ReportError(err, false)
}

func ReportPanic(err error) {
	ReportError(err, true)
}

func ReportError(err error, panic bool) {
	// report full stack trace - if this is too verbose we can restrict it
	stack := make([]byte, 1024)
	stack = stack[:runtime.Stack(stack, false)]
	stackTrace := string(stack)

	ErrorsChannel <- &ErrorReport{
		Error:      err,
		Panic:      panic,
		StackTrace: stackTrace,
	}
}

func extractErrorFromPanicRecover(panicError interface{}) error {
	var err error
	if e, ok := panicError.(error); ok {
		err = e
	} else {
		logger.Error("Panic without error")
	}

	return err
}
