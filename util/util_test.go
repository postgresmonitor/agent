package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRound(t *testing.T) {
	assert.Equal(t, 1.13, Round(1.1234))
}

func TestRound4(t *testing.T) {
	assert.Equal(t, 1.1235, Round4(1.12342343))
}

func TestParseTimestamp(t *testing.T) {
	assert.Equal(t, int64(1648166371), ParseTimestampToUnix("2022-03-24T23:59:31+00:00"))
}
