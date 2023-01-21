package db

import "agent/logger"

type SlowQuery struct {
	SqlErrorCode     string
	Metadata         string
	DurationMs       float64
	Raw              string
	Obfuscated       string
	Comment          string
	Explain          string
	Fingerprint      string
	ServerConfigName string
	MeasuredAt       int64
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
				return
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
				if client.serverID.ConfigName == slowQuery.ServerConfigName {
					serverID = client.serverID
					postgresClient = client
					break
				}
			}

			// obfuscate explains since the raw explain can contain query inputs
			explain := o.explainer.Explain(postgresClient, slowQuery)
			if len(explain) > 0 {
				slowQuery.Explain = o.obfuscator.ObfuscateExplain(explain)
				// if explain is empty, then the query was already explained the last hour
				logger.Debug("Slow Query", "duration_ms", slowQuery.DurationMs, "query", slowQuery.Raw, "obfuscated", slowQuery.Obfuscated, "fingerprint", slowQuery.Fingerprint, "explain", explain, "obfuscated_explain", slowQuery.Explain, "measured_at", slowQuery.MeasuredAt)
			}

			// report slow query stats
			slowQueryStats := &QueryStats{
				ServerID:    serverID,
				Fingerprint: slowQuery.Fingerprint,
				Query:       slowQuery.Obfuscated,
				Explain:     slowQuery.Explain,
				Calls:       1,
				TotalTime:   slowQuery.DurationMs,
				MinTime:     slowQuery.DurationMs,
				MaxTime:     slowQuery.DurationMs,
				MeasuredAt:  slowQuery.MeasuredAt,
			}
			select {
			case o.queryStatsChannel <- []*QueryStats{slowQueryStats}:
				// sent
			default:
				logger.Warn("Dropping query stats: channel buffer full")
			}
		}
	}
}
