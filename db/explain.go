package db

import (
	"agent/logger"
	"time"
)

type Explainer struct {
	// cache of query fingerprint to expiration time
	explained map[string]time.Time
}

func (e *Explainer) Explain(postgresClient *PostgresClient, slowQuery *SlowQuery) string {
	var explain string

	if e.explained == nil {
		e.explained = make(map[string]time.Time)
	}

	// only explain queries once per hour
	expiration, ok := e.explained[slowQuery.Fingerprint]
	if ok && time.Now().UTC().Before(expiration) {
		return explain
	}

	// TODO: add support for ANALYZE and BUFFERS as an opt-in config setting
	explainQuery := "EXPLAIN (SUMMARY true) " + slowQuery.Raw + postgresMonitorQueryCommentString
	rows, err := postgresClient.client.Query(explainQuery)

	if err != nil {
		logger.Error("Explain error", "err", err)
		return explain
	}
	defer rows.Close()

	for rows.Next() {
		var explainRow string
		err := rows.Scan(&explainRow)
		if err != nil {
			continue
		}
		explain += explainRow + "\n"
	}

	// add expiration for cached query fingerprint
	e.explained[slowQuery.Fingerprint] = time.Now().UTC().Add(1 * time.Hour)

	return explain
}
