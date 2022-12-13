package util

import (
	"postgres-monitor/logger"

	"github.com/hashicorp/go-version"
)

func VersionGreaterThan(v1 string, v2 string) bool {
	c, err := VersionCompare(v1, v2)
	if err != nil {
		logger.Error("Invalid version comparison", "v1", v1, "v2", v2)
		return false
	}

	return c > 0
}

func VersionGreaterThanOrEqual(v1 string, v2 string) bool {
	c, err := VersionCompare(v1, v2)
	if err != nil {
		logger.Error("Invalid version comparison", "v1", v1, "v2", v2)
		return false
	}

	return c >= 0
}

// returns -2 for invalid versions
func VersionCompare(v1 string, v2 string) (int, error) {
	version1, err := version.NewVersion(v1)
	if err != nil {
		return -2, err
	}

	version2, err := version.NewVersion(v2)
	if err != nil {
		return -2, err
	}

	return version1.Compare(version2), nil
}
