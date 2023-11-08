package db

import "agent/logger"

type SlowQuery struct {
	SqlErrorCode string
	Metadata     string
	DurationMs   float64
	Raw          string
	Obfuscated   string
	Comment      string
	Explain      string
	Fingerprint  string
	ServerName   string
	MeasuredAt   int64
}

// runs forever
func (o *Observer) MonitorSlowQueries() {
	for {
		select {
		case slowQuery := <-o.rawSlowQueryChannel:
			// parse out comment
			parsedComment := parseComment(slowQuery.Raw)
			slowQuery.Comment = parsedComment.Comment

			// skip any agent query if configured to
			if !o.config.MonitorAgentQueries && isAgentQueryComment(parsedComment.Comment) {
				continue
			}

			slowQuery.Raw = parsedComment.Query

			slowQuery.Obfuscated = o.obfuscator.ObfuscateQuery(slowQuery.Raw)
			// collapse spaces and clean chars after obfuscating but before fingerprinting
			// we obfuscate first to not collapse any run of spaces in a query string
			// although it shouldn't matter too much since we don't report raw query params
			slowQuery.Obfuscated = CleanQuery(slowQuery.Obfuscated)
			slowQuery.Fingerprint = fingerprintQuery(slowQuery.Obfuscated)

			// if query is more than 5000 chars then truncate and add TRUNCATED suffix
			if len(slowQuery.Obfuscated) > 5000 {
				slowQuery.Obfuscated = TruncateQuery(slowQuery.Obfuscated)
			}

			// find postgres client and serverId for query using config name
			var serverID *ServerID
			var postgresClient *PostgresClient
			for _, client := range o.postgresClients {
				if client.serverID.Name == slowQuery.ServerName {
					serverID = client.serverID
					postgresClient = client
					break
				}
			}

			rawExplain := slowQuery.Explain

			// don't explain query if explain already present - ex. from auto_explain
			if rawExplain == "" {
				// obfuscate explains since the raw explain can contain query inputs
				rawExplain = o.explainer.Explain(postgresClient, slowQuery)
				if len(rawExplain) > 0 {
					slowQuery.Explain = o.obfuscator.ObfuscateExplain(rawExplain)
					// if explain is empty, then the query was already explained the last hour
				}
			} else {
				slowQuery.Explain = o.obfuscator.ObfuscateExplain(rawExplain)
			}

			logger.Debug("Slow Query", "duration_ms", slowQuery.DurationMs, "query", slowQuery.Raw, "obfuscated", slowQuery.Obfuscated, "fingerprint", slowQuery.Fingerprint, "explain", rawExplain, "obfuscated_explain", slowQuery.Explain, "measured_at", slowQuery.MeasuredAt)

			// report slow query stats
			slowQueryStats := &QueryStats{
				ServerID:    serverID,
				Fingerprint: slowQuery.Fingerprint,
				Query:       slowQuery.Obfuscated,
				Comment:     slowQuery.Comment,
				Explain:     slowQuery.Explain,
				Calls:       1,
				TotalTime:   slowQuery.DurationMs,
				MinTime:     slowQuery.DurationMs,
				MaxTime:     slowQuery.DurationMs,
				MeasuredAt:  slowQuery.MeasuredAt,
			}
			select {
			case o.dataChannel <- []*QueryStats{slowQueryStats}:
				// sent
			default:
				logger.Warn("Dropping query stats: channel buffer full")
			}
		}
	}
}
