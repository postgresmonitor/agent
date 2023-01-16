package errors

import (
	stderrors "errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeferRecover(t *testing.T) {
	go func() {
		defer DeferRecover()
		panic(stderrors.New("Panic Error"))
	}()

	time.Sleep(1 * time.Second)

	select {
	case er := <-ErrorsChannel:
		assert.Equal(t, "Panic Error", er.Error.Error())
		assert.True(t, er.Panic)

		assert.True(t, strings.Contains(er.StackTrace, "goroutine"))
		assert.True(t, strings.Contains(er.StackTrace, "agent/errors/errors.go:"))
	default:
		assert.Fail(t, "No reported panic received")
	}
}

func TestDeferRecoverWithCallback(t *testing.T) {
	var callbackError error
	go func() {
		defer DeferRecoverWithCallback(func(err error) {
			callbackError = err
		})
		panic(stderrors.New("Panic Error"))
	}()

	time.Sleep(1 * time.Second)

	select {
	case er := <-ErrorsChannel:
		assert.Equal(t, "Panic Error", er.Error.Error())
		assert.True(t, er.Panic)

		assert.True(t, strings.Contains(er.StackTrace, "goroutine"))
		assert.True(t, strings.Contains(er.StackTrace, "agent/errors/errors.go:"))
	default:
		assert.Fail(t, "No reported error received")
	}

	time.Sleep(200 * time.Millisecond)

	assert.NotNil(t, callbackError)
	assert.Equal(t, "Panic Error", callbackError.Error())
}

func TestReport(t *testing.T) {
	go Report(stderrors.New("Test Error"))

	time.Sleep(1 * time.Second)

	select {
	case er := <-ErrorsChannel:
		assert.Equal(t, "Test Error", er.Error.Error())
		assert.False(t, er.Panic)

		assert.True(t, strings.Contains(er.StackTrace, "goroutine"))
		assert.True(t, strings.Contains(er.StackTrace, "agent/errors/errors.go:"))
	default:
		assert.Fail(t, "No reported error received")
	}
}
