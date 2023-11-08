package aws

import (
	"agent/db"
	"agent/logger"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var timestampRegex = regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC`)
var explainedQueryRegex = regexp.MustCompile(`duration:\s*(?P<duration>\d+\.\d+)\s*ms\s*plan:\s*Query Text:\s*`)

// examples
// 2023-10-28 19:01:19 UTC:123.456.789.123(57157):user@database:[13441]:ERROR:  Function pg_stat_get_wal_receiver() is currently not supported in Aurora
// 2023-10-28 19:01:19 UTC:123.456.789.123(57157):user_2@database:[13441]:STATEMENT:  select status, conninfo from pg_stat_wal_receiver;
// 2023-10-28 19:03:12 UTC:123.456.789.123(54896):user@database_name:[11818]:LOG:  could not receive data from client: Connection reset by peer
// 2023-10-28 19:20:14 UTC:123.456.789.123(55368):[unknown]@[unknown]:[15690]:LOG:  invalid length of startup packet
type RDSLogLine struct {
	Timestamp int64
	IP        net.IP
	Username  string
	Database  string
	Level     string
	Message   string
}

func ParseRDSLogFile(logFile string) []*RDSLogLine {
	var parsedLogLines []*RDSLogLine

	// parse raw lines into groups of related lines that go together
	logLines := StitchRDSLogLinesIntoChunks(logFile)

	for _, logLine := range logLines {

		parts := strings.Split(logLine, ":")
		if len(parts) > 6 {
			timestampToken := strings.Join(parts[0:3], ":")
			timestamp := ParseRDSLogLineTimestamp(timestampToken)

			ipString := strings.Split(parts[3], "(")[0] // remove (57157) from 123.456.789.123(57157)
			ip := net.ParseIP(ipString)

			userAndDatabase := strings.Split(parts[4], "@")

			message := strings.TrimSpace(strings.Join(parts[7:], ":"))

			rdsLogLine := &RDSLogLine{
				Timestamp: timestamp,
				IP:        ip,
				Username:  userAndDatabase[0],
				Database:  userAndDatabase[1],
				Level:     parts[6],
				Message:   message,
			}

			parsedLogLines = append(parsedLogLines, rdsLogLine)
		}
	}

	// split by : and take first token and parse timestamp
	return parsedLogLines
}

func StitchRDSLogLinesIntoChunks(logFile string) []string {
	var chunkedLines []string
	var currentChunkedLine string

	// split by \n but then chunk them into lines that start with a timestamp
	rawLines := strings.Split(logFile, "\n")
	if len(rawLines) == 0 {
		return chunkedLines
	}

	// keep adding lines to a single chunked lines until you find another timestamp
	for _, rawLine := range rawLines {
		if rawLine == "" {
			continue
		}

		firstToken := strings.Split(rawLine, "UTC:")[0]
		timestamp := ParseRDSLogLineTimestamp(firstToken)
		if timestamp != -1 {
			// valid timestamp
			if currentChunkedLine == "" {
				currentChunkedLine = rawLine
			} else {
				// add finished chunked line to lines
				chunkedLines = append(chunkedLines, currentChunkedLine)
				currentChunkedLine = rawLine
			}
		} else {
			// if not a timestamp, then append to currentChunkedLine
			currentChunkedLine += "\n" + rawLine
		}
	}

	// add last chunked line
	chunkedLines = append(chunkedLines, currentChunkedLine)

	return chunkedLines
}

func ParseRDSLogLineTimestamp(token string) int64 {
	// remove UTC to make timestamp parsing easier
	token = strings.ReplaceAll(token, " UTC", "")
	token = strings.TrimSpace(token)
	parsed, err := time.Parse(time.DateTime, token)
	if err != nil {
		return -1
	}

	return parsed.Unix()
}

func (o *Observer) ProcessRDSLogLines(rdsLogLines []*RDSLogLine, rdsInstanceID string) {
	if len(rdsLogLines) == 0 {
		return
	}

	for _, rdsLogLine := range rdsLogLines {
		slowQuery := ParseRDSQueryExplain(rdsLogLine, rdsInstanceID)

		if slowQuery == nil {
			continue
		}

		select {
		case o.slowQueryChannel <- slowQuery:
			// sent
		default:
			logger.Warn("Dropping AWS slow query: channel buffer full")
		}
	}
}

func ParseRDSQueryExplain(rdsLogLine *RDSLogLine, rdsInstanceID string) *db.SlowQuery {
	// verify that it's a explained query log line
	durationMatches := explainedQueryRegex.FindStringSubmatch(rdsLogLine.Message)

	if len(durationMatches) == 0 {
		return nil
	}

	parts := strings.Split(rdsLogLine.Message, "\n")

	query := strings.TrimSpace(strings.ReplaceAll(parts[1], "Query Text:", ""))
	if query == "" {
		return nil
	}

	explain := strings.Join(parts[2:], "\n")
	if explain == "" {
		return nil
	}

	// remove first \t if it starts with one
	if strings.HasPrefix(explain, "\t") {
		explain = explain[1:]
	}
	// replace other \t with 4 spaces and trim spaces from beginning and end
	explain = strings.TrimSpace(strings.ReplaceAll(explain, "\t", "    "))

	duration, _ := strconv.ParseFloat(durationMatches[1], 64)

	return &db.SlowQuery{
		DurationMs: duration,
		Raw:        query,
		Explain:    explain,
		MeasuredAt: rdsLogLine.Timestamp,
		ServerName: rdsInstanceID,
	}
}
