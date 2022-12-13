package logs

import (
	"regexp"
	"strings"
)

//
// These functions parse the syslog lines that are received from Heroku logs.
// Minimal processing is used with further processing in other parsers.
//

// we parse out color and segment because multiline logs need their messages to be stitched together
// and color and segment get in the way
var processRegex = `(?P<process>\w+(\.|-)\w+) -`
var optionalColorRegex = `\s?\[?(?P<color>\w+)?\]?`
var optionalSegmentRegex = `\s?\[?(?P<segment>\d+)?(-\d+)?\]?`
var logLineRegex = regexp.MustCompile(`\d+ <\d+>\d+ (?P<timestamp>.*) host app ` + processRegex + optionalColorRegex + optionalSegmentRegex + ` (?P<message>.*)`)

// parsed postgres log line with minimal processing
type SyslogLine struct {
	color     string // ex. GREEN - set for logs coming from specific databases
	message   string
	process   string // ex. postgres.12345
	segment   string // ex. 301 from [301-1]
	timestamp string // string unix timestamp
}

func parseSyslogLine(line string) []*SyslogLine {
	// split line by newline to handle when multiple lines are sent together
	lines := strings.Split(line, "\n")

	var syslogLines []*SyslogLine

	for _, line := range lines {
		var color string
		var message string
		var process string
		var segment string
		var timestamp string

		namedMatches := regexMatchSyslogLine(line)
		if namedMatches == nil {
			continue
		}

		// grouped messages always have the same color, timestamp, process, and segment
		color = namedMatches["color"]
		process = namedMatches["process"]
		segment = namedMatches["segment"]
		timestamp = namedMatches["timestamp"]
		message = namedMatches["message"]

		// final validation
		if message == "" {
			continue
		}

		var appendedToPrevousLine bool
		// append log messages to previous log line based on process
		for _, syslogLine := range syslogLines {
			// match log lines to process (ex. postgres.12345) and the same segment (ex. [123-1] and [123-2] but not [124-1])
			// and append messages to existing lines
			if syslogLine.process == process && syslogLine.segment == segment {
				syslogLine.message += message
				appendedToPrevousLine = true
				break
			}
		}

		if !appendedToPrevousLine {
			syslogLine := &SyslogLine{
				color:     color,
				message:   message,
				process:   process,
				segment:   segment,
				timestamp: timestamp,
			}
			syslogLines = append(syslogLines, syslogLine)
		}
	}

	return syslogLines
}

func regexMatchSyslogLine(line string) map[string]string {
	match := logLineRegex.FindStringSubmatch(line)
	if match == nil {
		return nil
	}

	namedMatches := make(map[string]string)
	for i, name := range logLineRegex.SubexpNames() {
		if i != 0 && name != "" {
			namedMatches[name] = match[i]
		}
	}

	// basic validation
	if len(namedMatches) < 2 {
		return nil
	}

	return namedMatches
}
