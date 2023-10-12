package logs

import (
	"agent/util"
	"fmt"
	"strings"
)

func parseHerokuPostgresKeyValueLogLine(line *SyslogLine) *ParsedLogLine {
	keyValues := map[string]string{}

	// convert to string unix timestamp
	keyValues["timestamp"] = fmt.Sprint(util.ParseTimestampToUnix(line.timestamp))

	parts := strings.Fields(line.message)

	for _, part := range parts {
		if len(part) < 2 {
			continue
		}

		keyValue := strings.Split(part, "=")

		if len(keyValue) != 2 {
			continue
		}

		key := keyValue[0]
		key = strings.ReplaceAll(key, "sample#", "") // remove all sample# prefixes
		keyValues[key] = keyValue[1]
	}

	return &ParsedLogLine{
		Metrics: keyValues,
	}
}
