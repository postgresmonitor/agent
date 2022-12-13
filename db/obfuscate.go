package db

import (
	"regexp"
	"strings"
)

// query
var caseInsensitiveRegex = `(?i)`
var operatorRegex = `(?P<operator>(=|>|<|i?like|limit|select|offset|then)\s*)`
var quotedParamRegex = `'[^']+'` // single quote + any non single quote + single quote
var numberParamRegex = `\d+\.?\d*`
var bindParamRegex = `\$\d+`
var paramRegex = `(?P<param>\s*` + quotedParamRegex + `|` + numberParamRegex + `|` + bindParamRegex + `)`
var queryOperatorParamRegex = regexp.MustCompile(caseInsensitiveRegex + operatorRegex + paramRegex)

// for both in and values lists - ex. IN (1, 2) and VALUES (1, 2)
var listOperatorRegex = `(?P<operator>\b(in|values)\s*)`
var listValuesParamRegex = `(?P<param>\(('?.+?'|'?.+?'?)\)(,\s*\(('?.+?'|'?.+?'?)\))*)` // (1,2) with optional repeating ,(1,2)
var queryListParamRegex = regexp.MustCompile(caseInsensitiveRegex + listOperatorRegex + listValuesParamRegex)

var betweenOperatorRegex = `(?P<operator>\bbetween(\s*symmetric)?\s*)`
var betweenParam1Regex = `(?P<param1>` + quotedParamRegex + `|` + numberParamRegex + `|` + bindParamRegex + `)`
var betweenAndRegex = `(?P<and>\s*AND\s*)`
var betweenParam2Regex = `(?P<param2>` + quotedParamRegex + `|` + numberParamRegex + `|` + bindParamRegex + `)`
var queryBetweenParamRegex = regexp.MustCompile(caseInsensitiveRegex + betweenOperatorRegex + betweenParam1Regex + betweenAndRegex + betweenParam2Regex)

// explain
var explainParamLineStartRegex = regexp.MustCompile(caseInsensitiveRegex + `(Cond:|Filter:)`)
var explainOperatorRegex = `(?P<operator>(=|>|<|~~|ANY \()\s*)`
var explainOperatorParamRegex = regexp.MustCompile(caseInsensitiveRegex + explainOperatorRegex + paramRegex)

type Obfuscator struct {
}

func NewObfuscator() *Obfuscator {
	return &Obfuscator{}
}

// obfuscate query input by replacing with ?
// works on both user input and bind params (ex. $1)
// query comment should be removed before obfuscation
func (o *Obfuscator) ObfuscateQuery(query string) string {
	query = queryOperatorParamRegex.ReplaceAllString(query, "$operator?")
	query = queryListParamRegex.ReplaceAllString(query, "$operator(?)")
	query = queryBetweenParamRegex.ReplaceAllString(query, "$operator?$and?")

	return query
}

// obfuscate explain plans by replacing query params with ?
func (o *Obfuscator) ObfuscateExplain(explain string) string {
	if len(explain) == 0 {
		return explain
	}

	var obfuscated string
	lines := strings.Split(explain, "\n")
	numLines := len(lines)
	for index, line := range lines {
		// if line starts with Cond: or Filter:
		if explainParamLineStartRegex.MatchString(line) {
			obfuscatedLine := explainOperatorParamRegex.ReplaceAllString(line, "$operator?")
			obfuscated += obfuscatedLine
		} else {
			obfuscated += line
		}
		if index != numLines-1 {
			obfuscated += "\n"
		}
	}
	return obfuscated
}
