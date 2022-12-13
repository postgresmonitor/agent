package db

import (
	"os"
)

const (
	HerokuPlatform  = "heroku"
	UnknownPlatform = "unknown"
)

func GetPlatform(postgresClient *PostgresClient) string {
	if IsHerokuPlatform(postgresClient) {
		return HerokuPlatform
	}
	return UnknownPlatform
}

func IsHerokuPlatform(postgresClient *PostgresClient) bool {
	if os.Getenv("DYNO") != "" {
		return true
	} else if HasHerokuSchema(postgresClient) {
		return true
	}

	return false
}

func HasHerokuSchema(postgresClient *PostgresClient) bool {
	var hasSchema bool

	query := "select 1 from information_schema.schemata where schema_name = 'heroku_ext'"

	err := postgresClient.client.QueryRow(query).Scan(&hasSchema)
	if err != nil {
		hasSchema = false
	}

	return hasSchema
}
