package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareInvalidVersions(t *testing.T) {
	c, err := VersionCompare("", "1.0")

	assert.Equal(t, -2, c)
	assert.NotNil(t, err)

	c, err = VersionCompare("1.0", "")

	assert.Equal(t, -2, c)
	assert.NotNil(t, err)
}

func TestCompare(t *testing.T) {
	c, _ := VersionCompare("1.0", "1.0")
	assert.Equal(t, 0, c)

	c, _ = VersionCompare("1.1", "1.0")
	assert.Equal(t, 1, c)

	c, _ = VersionCompare("2.1", "1.0")
	assert.Equal(t, 1, c)

	c, _ = VersionCompare("2.1", "13.0")
	assert.Equal(t, -1, c)
}

func TestVersionGreaterThan(t *testing.T) {
	assert.True(t, VersionGreaterThan("1.0", "0.1"))
	assert.True(t, VersionGreaterThan("14.0", "13.24"))
	assert.True(t, VersionGreaterThan("10.22", "9.5.6"))

	assert.False(t, VersionGreaterThan("9.5", "10.22"))
	assert.False(t, VersionGreaterThan("14.0", "15.1"))
}

func TestVersionGreaterThanOrEqual(t *testing.T) {
	assert.True(t, VersionGreaterThanOrEqual("1.0", "0.1"))
	assert.True(t, VersionGreaterThanOrEqual("14.0", "13.24"))
	assert.True(t, VersionGreaterThanOrEqual("10.22", "9.5.6"))
	assert.True(t, VersionGreaterThanOrEqual("10.22", "10.22"))

	assert.False(t, VersionGreaterThanOrEqual("9.5", "10.22"))
	assert.False(t, VersionGreaterThanOrEqual("14.0", "15.1"))
}
