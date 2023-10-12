package logs

import (
	"agent/db"
	"strings"
)

const appPostgres = "app postgres."
const herokuPostgres = "heroku-postgres"
const herokuPGBouncer = "heroku-pgbouncer"
const postgresProcessPrefix = "postgres."

const logTestMessage = "NOTICE:  POSTGRES_MONITOR_AGENT_TEST"

// struct that is returned
type ParsedLogLine struct {
	Metrics   map[string]string
	SlowQuery *db.SlowQuery
}

func shouldHandleTestLogLine(line string) bool {
	if len(line) == 0 {
		return false
	}

	return strings.Contains(line, logTestMessage)
}

func shouldHandleLogLine(line string) bool {
	if len(line) == 0 {
		return false
	}

	// benchmarked using a regex and contains was faster with no allocations
	return isAppPostgresStringLogLine(line) || isHerokuPostgresStringLogLine(line) || isHerokuPGBouncerStringLogLine(line)
}

func isAppPostgresStringLogLine(line string) bool {
	return strings.Contains(line, appPostgres)
}

func isAppPostgresSyslogLine(line *SyslogLine) bool {
	return line.process == appPostgres
}

func isHerokuPostgresStringLogLine(line string) bool {
	return strings.Contains(line, herokuPostgres)
}

func isHerokuPostgresSyslogLine(line *SyslogLine) bool {
	return line.process == herokuPostgres
}

func isHerokuPGBouncerStringLogLine(line string) bool {
	return strings.Contains(line, herokuPGBouncer)
}

// we've turned off heroku pg bouncer log line metrics since
// they are flaky and sometimes heroku doesn't log them at all
// we track pgbouncer metrics directly so these are unnecessary for now
func isHerokuPGBouncerSyslogLine(line *SyslogLine) bool {
	return false
	// return line.process == herokuPGBouncer
}

func isSqlSyslogLine(line *SyslogLine) bool {
	return strings.HasPrefix(line.process, postgresProcessPrefix)
}

// Parsing log lines happens in two passes.
// First the line is parsed into a syslogline with some simple structure
// Next, metrics or queries are parsed out of the syslogline.
// A single received log line may be several new line delimited lines
func parseLogLine(line string) []*ParsedLogLine {
	syslogLines := parseSyslogLine(line)
	if syslogLines == nil {
		return nil
	}

	var parsedLogLines []*ParsedLogLine

	for _, syslogLine := range syslogLines {
		var parsed *ParsedLogLine

		switch {
		case isHerokuPostgresSyslogLine(syslogLine):
			parsed = parseHerokuPostgresKeyValueLogLine(syslogLine)
		case isHerokuPGBouncerSyslogLine(syslogLine):
			parsed = parseHerokuPostgresKeyValueLogLine(syslogLine)
		case isSqlSyslogLine(syslogLine):
			parsed = parseSqlSyslogLine(syslogLine)
		}

		if parsed != nil {
			parsedLogLines = append(parsedLogLines, parsed)
		}
	}

	return parsedLogLines
}
