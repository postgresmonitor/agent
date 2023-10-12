package logs

import (
	"agent/db"
	"agent/util"
	"regexp"
	"strconv"
	"strings"
)

// error codes: https://www.postgresql.org/docs/current/errcodes-appendix.html
var errorRegex = `(?P<error>\d+)\s*`
var metadataRegex = `(?P<metadata>.*)\s*`
var durationRegex = `LOG:\s+duration:\s+(?P<duration>\d+\.\d+) ms`
var queryRegex = `\s+execute <\w+>: (?P<query>.*)`
var sqlLogLineRegex = regexp.MustCompile(`sql_error_code = ` + errorRegex + metadataRegex + durationRegex + queryRegex)

// NOTE: there are lots of different sql log line formats - ex. DETAIL:, ERROR: and STATEMENT:
// for slow queries we only care about LOG: duration ...
// LOG: checkpoint is another format, etc
func parseSqlSyslogLine(line *SyslogLine) *ParsedLogLine {
	timestamp := util.ParseTimestampToUnix(line.timestamp)

	message := strings.TrimSpace(line.message)
	captureGroups := matchRegexSqlMessage(message)

	if len(captureGroups) == 0 {
		return nil
	}

	duration, _ := strconv.ParseFloat(captureGroups["duration"], 64)
	slowQuery := &db.SlowQuery{
		SqlErrorCode: captureGroups["error"],
		Metadata:     strings.TrimSpace(captureGroups["metadata"]),
		DurationMs:   duration,
		Raw:          captureGroups["query"],
		ServerName:   line.color,
		MeasuredAt:   timestamp,
	}

	return &ParsedLogLine{
		SlowQuery: slowQuery,
	}
}

func matchRegexSqlMessage(message string) map[string]string {
	captureGroups := make(map[string]string)

	match := sqlLogLineRegex.FindStringSubmatch(message)
	if len(match) == 0 {
		return captureGroups
	}

	for i, name := range sqlLogLineRegex.SubexpNames() {
		// the first match is the full message
		if i != 0 && name != "" {
			captureGroups[name] = match[i]
		}
	}

	return captureGroups
}
