package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatsNoValues(t *testing.T) {
	stats := &Stats{}

	assert.Equal(t, 0, stats.data["foo"])
}

func TestStatsIncrement(t *testing.T) {
	stats := &Stats{}

	stats.Increment("foo")

	assert.Equal(t, 1, stats.data["foo"])
	assert.Equal(t, 0, stats.data["bar"])

	stats.Increment("foo")
	stats.Increment("bar")
	assert.Equal(t, 2, stats.data["foo"])
	assert.Equal(t, 1, stats.data["bar"])
}

func TestStatsIncrementBy(t *testing.T) {
	stats := &Stats{}

	stats.IncrementBy("foo", 1)

	assert.Equal(t, 1, stats.data["foo"])
	assert.Equal(t, 0, stats.data["bar"])

	stats.IncrementBy("foo", 2)
	stats.IncrementBy("bar", 5)
	assert.Equal(t, 3, stats.data["foo"])
	assert.Equal(t, 5, stats.data["bar"])
}

func TestStatsToMap(t *testing.T) {
	stats := &Stats{}

	assert.Equal(t, make(map[string]int), stats.ToMap())

	stats.IncrementBy("foo", 1)
	stats.IncrementBy("bar", 5)

	expected := map[string]int{
		"foo": 1,
		"bar": 5,
	}
	assert.Equal(t, expected, stats.ToMap())
}

func TestStatsCopyAndResetEmpty(t *testing.T) {
	stats := &Stats{}

	copy := stats.CopyAndReset()

	assert.Equal(t, make(map[string]int), copy.ToMap())
	assert.Equal(t, make(map[string]int), stats.ToMap())
}

func TestStatsCopyAndReset(t *testing.T) {
	stats := &Stats{}

	stats.IncrementBy("foo", 1)
	stats.IncrementBy("bar", 5)

	copy := stats.CopyAndReset()

	expected := map[string]int{
		"foo": 1,
		"bar": 5,
	}
	assert.Equal(t, expected, copy.ToMap())

	assert.Equal(t, make(map[string]int), stats.ToMap())
}
