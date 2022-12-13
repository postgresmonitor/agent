package db

import (
	"regexp"
	"strings"
)

var commentRegex = regexp.MustCompile(`(((/\*)+?[\w\W]+?(\*/)+))`)
var postgresMonitorQueryCommentKeyValueString = "/* app:postgres-monitor-agent */"
var postgresMonitorQueryCommentString = " " + postgresMonitorQueryCommentKeyValueString

type ParsedComment struct {
	Comment string
	Query   string
}

func parseComment(query string) *ParsedComment {
	var comment string

	// multiple comments could appear so we append them together
	comments := commentRegex.FindAllString(query, -1)
	for _, c := range comments {
		query = strings.TrimSpace(strings.Replace(query, c, "", 1))

		comment += c
	}

	// truncate comment if it's too big
	if len(comment) > 1000 {
		comment = comment[0:1000] + TruncatedString
	}

	return &ParsedComment{
		Comment: comment,
		Query:   query,
	}
}

// add a query comment so users can easily identify which
// queries are coming from the postgres monitor agent
func postgresMonitorQueryComment() string {
	return postgresMonitorQueryCommentString
}

// a query is from the agent if it has the postgresMonitorQueryCommentString in the comment
func isAgentQueryComment(comment string) bool {
	return strings.Contains(comment, postgresMonitorQueryCommentKeyValueString)
}
