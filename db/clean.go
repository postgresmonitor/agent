package db

import "strings"

// remove \t and \n chars and collapse spaces
func CleanQuery(query string) string {
	cleanedRunes := make([]rune, 0, len(query))
	for _, r := range query {
		if r == '\n' || r == '\t' {
			// replace with space
			cleanedRunes = append(cleanedRunes, ' ')
		} else {
			cleanedRunes = append(cleanedRunes, r)
		}
	}
	cleaned := string(cleanedRunes)
	// replace multiple spaces into one
	return strings.Join(strings.Fields(cleaned), " ")
}
