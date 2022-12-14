package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEnvVar(t *testing.T) {
	assert.Equal(t, "foo", getEnvVar("FOO", "foo"))
	assert.Equal(t, "", getEnvVar("FOO", ""))

	os.Setenv("FOO", "bar")
	assert.Equal(t, "bar", getEnvVar("FOO", "foo"))
	assert.Equal(t, "bar", getEnvVar("FOO", ""))

	os.Unsetenv("FOO")
}

func TestGetEnvVarBool(t *testing.T) {
	assert.False(t, getEnvVarBool("FOO", false))
	assert.True(t, getEnvVarBool("FOO", true))

	os.Setenv("FOO", "false")
	assert.False(t, getEnvVarBool("FOO", false))
	assert.False(t, getEnvVarBool("FOO", true))

	os.Setenv("FOO", "true")
	assert.True(t, getEnvVarBool("FOO", false))
	assert.True(t, getEnvVarBool("FOO", true))

	os.Unsetenv("FOO")
}
