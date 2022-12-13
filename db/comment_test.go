package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCommentTrailingComment(t *testing.T) {
	expected := &ParsedComment{
		Comment: "/* comment */",
		Query:   "SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10",
	}
	parsed := parseComment("SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10 /* comment */")

	assert.Equal(t, expected, parsed)
}

func TestParseCommentLeadingComment(t *testing.T) {
	expected := &ParsedComment{
		Comment: "/* comment */",
		Query:   "SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10",
	}
	parsed := parseComment("/* comment */ SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10")

	assert.Equal(t, expected, parsed)
}

func TestParseCommentKeyValueComment(t *testing.T) {
	expected := &ParsedComment{
		Comment: "/*app:worker-foo,job:AlertWorker*/",
		Query:   "SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10",
	}
	parsed := parseComment("SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10/*app:worker-foo,job:AlertWorker*/")

	assert.Equal(t, expected, parsed)
}

func TestParseCommentMultipleComments(t *testing.T) {
	expected := &ParsedComment{
		Comment: "/*app:worker-foo*//* another comment*/",
		Query:   "SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10",
	}
	parsed := parseComment("SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10/*app:worker-foo*//* another comment*/")

	assert.Equal(t, expected, parsed)
}

func TestParseCommentMultipleCommentsSpace(t *testing.T) {
	expected := &ParsedComment{
		Comment: "/*app:worker-foo*//* another comment*/",
		Query:   "SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10",
	}
	parsed := parseComment("SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10/*app:worker-foo*/ /* another comment*/")

	assert.Equal(t, expected, parsed)
}

func TestParseCommentMultipleCommentsBeforeAndAfter(t *testing.T) {
	expected := &ParsedComment{
		Comment: "/*app:worker-foo*//* another comment */",
		Query:   "SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10",
	}
	parsed := parseComment("/*app:worker-foo*/SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10/* another comment */")

	assert.Equal(t, expected, parsed)
}

func TestIsAgentQueryComment(t *testing.T) {
	assert.False(t, isAgentQueryComment(""))
	assert.False(t, isAgentQueryComment("foo"))
	assert.False(t, isAgentQueryComment("/* app:foo */"))
	assert.True(t, isAgentQueryComment("/* app:postgres-monitor-agent */"))
}
