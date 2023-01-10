package db

import (
	"agent/logger"
	"agent/util"
	"database/sql"
	"math"
	"regexp"
	"sort"
	"time"
)

const MissingQueryString = "MISSING"
const RedactedString = "REDACTED"
const TruncatedString = "TRUNCATED"

var ipAddressRegex = regexp.MustCompile(`(?:[0-9]{1,3}\.){3}[0-9]{1,3}`)

// stateful stats object that stores all pg_stat_statement queries per database
// each minute the top queries per category (total time, slowest time, most called, slowest i/o)
// are sent with delta fields calculated from the past stats period
type QueryStatsState struct {
	// map of server config name + database to query stats
	Stats map[ServerID][]*QueryStats
}

// From https://www.postgresql.org/docs/current/sql-explain.html under BUFFERS
// Shared blocks contain data from regular tables and indexes; local blocks contain data from temporary tables and indexes;
// while temp blocks contain short-term working data used in sorts, hashes, Materialize plan nodes, and similar cases.
// The number of blocks dirtied indicates the number of previously unmodified blocks that were changed by this query;
// while the number of blocks written indicates the number of previously-dirtied blocks evicted from cache by this backend
// during query processing
type QueryStats struct {
	QueryId             int64
	Fingerprint         string
	ServerID            *ServerID
	Query               string
	Comment             string
	Explain             string
	Calls               int64
	TotalTime           float64
	MinTime             float64
	MaxTime             float64
	MeanTime            float64
	Rows                int64
	SharedBlocksHit     int64
	SharedBlocksRead    int64
	SharedBlocksDirtied int64
	SharedBlocksWritten int64
	LocalBlocksHit      int64
	LocalBlocksRead     int64
	LocalBlocksDirtied  int64
	LocalBlocksWritten  int64
	TempBlocksRead      int64
	TempBlocksWritten   int64
	BlockReadTime       float64
	BlockWriteTime      float64
	TotalBlockIOTime    float64
	MeasuredAt          int64
}

func (s *QueryStats) Delta(latest *QueryStats) *QueryStats {
	stats := &QueryStats{
		QueryId:             latest.QueryId,
		Fingerprint:         latest.Fingerprint,
		ServerID:            latest.ServerID,
		Query:               latest.Query,
		Comment:             latest.Comment,
		Explain:             latest.Explain,
		Calls:               latest.Calls - s.Calls,
		TotalTime:           latest.TotalTime - s.TotalTime,
		MinTime:             latest.MinTime,
		MaxTime:             latest.MaxTime,
		Rows:                latest.Rows - s.Rows,
		SharedBlocksHit:     latest.SharedBlocksHit - s.SharedBlocksHit,
		SharedBlocksRead:    latest.SharedBlocksRead - s.SharedBlocksRead,
		SharedBlocksDirtied: latest.SharedBlocksDirtied - s.SharedBlocksDirtied,
		SharedBlocksWritten: latest.SharedBlocksWritten - s.SharedBlocksWritten,
		LocalBlocksHit:      latest.LocalBlocksHit - s.LocalBlocksHit,
		LocalBlocksRead:     latest.LocalBlocksRead - s.LocalBlocksRead,
		LocalBlocksDirtied:  latest.LocalBlocksDirtied - s.LocalBlocksDirtied,
		LocalBlocksWritten:  latest.LocalBlocksWritten - s.LocalBlocksWritten,
		TempBlocksRead:      latest.TempBlocksRead - s.TempBlocksRead,
		TempBlocksWritten:   latest.TempBlocksWritten - s.TempBlocksWritten,
		BlockReadTime:       latest.BlockReadTime - s.BlockReadTime,
		BlockWriteTime:      latest.BlockWriteTime - s.BlockWriteTime,
		MeasuredAt:          latest.MeasuredAt,
	}
	stats.MeanTime = util.Percent(stats.TotalTime, float64(stats.Calls)) // mean for last time interval
	stats.TotalBlockIOTime = stats.BlockReadTime + stats.BlockWriteTime  // total block io time for last interval
	return stats
}

// Aggregate two query stats that have the same fingerprint together
// ex. max(maxTime), min(minTime), sum(calls), sum(time), etc
func (s *QueryStats) Aggregate(other *QueryStats) {
	s.Calls = s.Calls + other.Calls
	s.TotalTime = s.TotalTime + other.TotalTime
	s.MinTime = math.Min(s.MinTime, other.MinTime)
	s.MaxTime = math.Max(s.MaxTime, other.MaxTime)
	s.MeanTime = util.Percent(s.TotalTime, float64(s.Calls))
	s.Rows = s.Rows + other.Rows
	s.SharedBlocksHit = s.SharedBlocksHit + other.SharedBlocksHit
	s.SharedBlocksRead = s.SharedBlocksRead + other.SharedBlocksRead
	s.SharedBlocksDirtied = s.SharedBlocksDirtied + other.SharedBlocksDirtied
	s.SharedBlocksWritten = s.SharedBlocksWritten + other.SharedBlocksWritten
	s.LocalBlocksHit = s.LocalBlocksHit + other.LocalBlocksHit
	s.LocalBlocksRead = s.LocalBlocksRead + other.LocalBlocksRead
	s.LocalBlocksDirtied = s.LocalBlocksDirtied + other.LocalBlocksDirtied
	s.LocalBlocksWritten = s.LocalBlocksWritten + other.LocalBlocksWritten
	s.TempBlocksRead = s.TempBlocksRead + other.TempBlocksRead
	s.TempBlocksWritten = s.TempBlocksWritten + other.TempBlocksWritten
	s.BlockReadTime = s.BlockReadTime + other.BlockReadTime
	s.BlockWriteTime = s.BlockWriteTime + other.BlockWriteTime
	s.TotalBlockIOTime = s.BlockReadTime + s.BlockWriteTime                          // total block io time for last interval
	s.MeasuredAt = int64(math.Max(float64(s.MeasuredAt), float64(other.MeasuredAt))) // take the maximum measured at
}

// check for negative total time and other values to see if pg_stat_statements was reset
// or if the pg_stat_statements.max threshold was crossed, in which case
// the least-executed statements are dropped from pg_stat_statements.
// both cases mean that the current stats may have smaller values than
// the previous stats and we should toss out the current stats
// to prevent having confusing negative values.
func (s *QueryStats) Valid() bool {
	return s.TotalTime > 0 && s.BlockReadTime >= 0 && s.BlockWriteTime >= 0 && s.Calls > 0 && s.Rows >= 0 &&
		s.SharedBlocksDirtied >= 0 && s.SharedBlocksHit >= 0 && s.SharedBlocksRead >= 0 && s.SharedBlocksWritten >= 0 && s.TotalBlockIOTime >= 0
}

type QueryStatsMonitor struct {
	// stateful stats for the life of the process
	queryStatsState     *QueryStatsState
	queryStatsChannel   chan []*QueryStats
	obfuscator          *Obfuscator
	monitorAgentQueries bool
}

func (m *QueryStatsMonitor) Run(postgresClient *PostgresClient) {
	// initialize map
	if m.queryStatsState.Stats == nil {
		m.queryStatsState.Stats = make(map[ServerID][]*QueryStats)
	}

	currentStatsList := m.QueryForStats(postgresClient)

	// initialize database list
	previousStatsList, ok := m.queryStatsState.Stats[*postgresClient.serverID]

	var deltaStatsList []*QueryStats

	if ok {
		// merge previous stats with current to compute change/delta fields
		// but use current total values
		// merge by query id before we aggregate by fingerprint
		for _, previousStats := range previousStatsList {
			for _, currentStats := range currentStatsList {
				if previousStats.QueryId == currentStats.QueryId {
					delta := previousStats.Delta(currentStats)
					// check for valid query stats to see if pg_stat_statements were reset
					// or if the pg_stat_statements.max threshold was crossed, in which case
					// the least-executed statements are dropped from pg_stat_statements.
					// both cases mean that the current stats may have smaller values than
					// the previous stats and we should toss out the current stats
					// to prevent having confusing negative values.
					if delta.Valid() {
						deltaStatsList = append(deltaStatsList, delta)
					}
					break
				}
			}
		}
	} else {
		// only report query stats once the stats object has a delta from two consecutive polls
		m.queryStatsState.Stats[*postgresClient.serverID] = currentStatsList
		return
	}

	m.queryStatsState.Stats[*postgresClient.serverID] = currentStatsList

	// aggregate delta stats by fingerprint to remove dupes
	aggregated := m.AggregateStats(deltaStatsList)

	// filter to the top 100 worst queries by category - don't send all of the queries
	filtered := m.FilterStats(aggregated)

	// report aggregated stats to channel
	select {
	case m.queryStatsChannel <- filtered:
		// sent
	default:
		logger.Warn("Dropping query stats: channel buffer full")
	}
}

func (m *QueryStatsMonitor) QueryForStats(postgresClient *PostgresClient) []*QueryStats {
	var timeFields string
	// if postgres 13 or greater use the newer field names
	if util.VersionGreaterThanOrEqual(postgresClient.version, "13.0") {
		// TODO: track plan time and wal time stats for newer versions of postgres
		// https://www.postgresql.org/docs/13/pgstatstatements.html
		timeFields = "total_exec_time, min_exec_time, max_exec_time, "
	} else {
		// https://www.postgresql.org/docs/10/pgstatstatements.html
		timeFields = "total_time, min_time, max_time, "
	}

	// pg_stat_statements should always be enabled on heroku postgres servers
	query := `select queryid, query, calls, ` + timeFields + `
						rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit,
						local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
						blk_read_time, blk_write_time
						from pg_stat_statements stat
						join pg_database pdb on pdb.oid = stat.dbid
						where pdb.datname = current_database()` + postgresMonitorQueryComment()

	rows, err := postgresClient.client.Query(query)

	if err != nil {
		return []*QueryStats{}
	}
	defer rows.Close()

	var allStats []*QueryStats
	measuredAt := time.Now().UTC().Unix()

	for rows.Next() {
		var newStats QueryStats
		var nullableQuery sql.NullString

		err := rows.Scan(
			&newStats.QueryId,
			&nullableQuery,
			&newStats.Calls,
			&newStats.TotalTime,
			&newStats.MinTime,
			&newStats.MaxTime,
			&newStats.Rows,
			&newStats.SharedBlocksHit,
			&newStats.SharedBlocksRead,
			&newStats.SharedBlocksDirtied,
			&newStats.SharedBlocksWritten,
			&newStats.LocalBlocksHit,
			&newStats.LocalBlocksRead,
			&newStats.LocalBlocksDirtied,
			&newStats.LocalBlocksWritten,
			&newStats.TempBlocksRead,
			&newStats.TempBlocksWritten,
			&newStats.BlockReadTime,
			&newStats.BlockWriteTime,
		)
		if err != nil {
			continue
		}

		newStats.ServerID = postgresClient.serverID
		newStats.MeasuredAt = measuredAt

		// if query is null then pg_stat_statements discard query text because there are
		// too many queries being tracked. Reduce pg_stat_statements.max to fix this
		if !nullableQuery.Valid {
			newStats.Query = MissingQueryString
		} else {
			query := m.Redact(nullableQuery.String)

			// parse the comment, obfuscate query and fingerprint it
			parsedComment := parseComment(query)
			newStats.Comment = parsedComment.Comment

			// skip any agent query if configured to
			if !m.monitorAgentQueries && isAgentQueryComment(parsedComment.Comment) {
				continue
			}

			query = parsedComment.Query

			obfuscated := m.obfuscator.ObfuscateQuery(query)
			// collapse spaces and clean chars after obfuscating but before fingerprinting
			// we obfuscate first to not collapse any run of spaces in a query string - although
			// it shouldn't matter too much since we don't report raw query params
			obfuscated = CleanQuery(obfuscated)

			newStats.Fingerprint = fingerprintQuery(obfuscated)
			newStats.Query = obfuscated
		}

		// if query is more than 5000 chars then truncate and add TRUNCATED suffix
		if len(newStats.Query) > 5000 {
			newStats.Query = TruncateQuery(newStats.Query)
		}

		allStats = append(allStats, &newStats)
	}

	return allStats
}

// redact the given query by filtering out ip addresses, etc
func (m *QueryStatsMonitor) Redact(query string) string {
	return ipAddressRegex.ReplaceAllString(query, RedactedString)
}

func TruncateQuery(query string) string {
	return query[0:5000] + TruncatedString
}

// aggregate queries by fingerprint since pg_stat_statements will have duplicate
// queries due to different query shapes such as IN (1, 2) being possibly different from IN (1, 2, 3)
// obfuscated queries will all look the same and have the same fingerprint
func (m *QueryStatsMonitor) AggregateStats(queryStats []*QueryStats) []*QueryStats {
	aggregatedByQueryFingerprint := make(map[string]*QueryStats)

	// put stats in aggregated map - if already present, aggregate it
	for _, stats := range queryStats {
		if _, ok := aggregatedByQueryFingerprint[stats.Fingerprint]; !ok {
			aggregatedByQueryFingerprint[stats.Fingerprint] = stats
		} else {
			aggregatedByQueryFingerprint[stats.Fingerprint].Aggregate(stats)
		}
	}

	// take all aggregated query stats by fingerprint
	var aggregated []*QueryStats
	for _, stats := range aggregatedByQueryFingerprint {
		aggregated = append(aggregated, stats)
	}

	return aggregated
}

// filter queries by calls, slowest avg time, total time and slowest i/o
func (m *QueryStatsMonitor) FilterStats(queryStats []*QueryStats) []*QueryStats {
	var missingQueryText bool
	var missingQueryServerID *ServerID
	filteredByQueryFingerprint := make(map[string]*QueryStats)

	// filter out queries without calls
	var called []*QueryStats
	for _, stats := range queryStats {
		// don't track query stats for missing query text but add placeholder stats
		// to report the missing queries to the backend
		if stats.Query == MissingQueryString && !missingQueryText {
			missingQueryText = true
			missingQueryServerID = stats.ServerID
			continue
		}

		// send up a query only when it is called
		// this means we don't report query stats the first monitor call
		if stats.Calls > 0 {
			called = append(called, stats)
		}
	}

	// if missing the query text, add an empty query stats object to report this
	if missingQueryText {
		filteredByQueryFingerprint["0"] = &QueryStats{
			Query:    MissingQueryString,
			ServerID: missingQueryServerID,
		}
	}

	// filter to top 25 queries per most called
	sort.Slice(called, func(i, j int) bool {
		return called[i].Calls > called[j].Calls
	})
	count := 0
	for _, stats := range called {
		if count >= 25 {
			break
		}
		if _, ok := filteredByQueryFingerprint[stats.Fingerprint]; !ok {
			filteredByQueryFingerprint[stats.Fingerprint] = stats
			count += 1
		}
	}

	// top 25 by total time
	sort.Slice(called, func(i, j int) bool {
		return called[i].TotalTime > called[j].TotalTime
	})
	count = 0
	for _, stats := range called {
		if count >= 25 {
			break
		}
		if _, ok := filteredByQueryFingerprint[stats.Fingerprint]; !ok {
			filteredByQueryFingerprint[stats.Fingerprint] = stats
			count += 1
		}
	}

	// top 25 by slowest time
	sort.Slice(called, func(i, j int) bool {
		return called[i].MeanTime > called[j].MeanTime
	})
	count = 0
	for _, stats := range called {
		// only include queries that take more than 10 ms
		if stats.MeanTime < 10 {
			continue
		}
		if count >= 25 {
			break
		}
		if _, ok := filteredByQueryFingerprint[stats.Fingerprint]; !ok {
			filteredByQueryFingerprint[stats.Fingerprint] = stats
			count += 1
		}
	}

	// top 25 by slowest i/o
	sort.Slice(called, func(i, j int) bool {
		// slowest i/o is block read time + write time
		return called[i].TotalBlockIOTime > called[j].TotalBlockIOTime
	})
	count = 0
	for _, stats := range called {
		// only include queries that have more than 10 ms of io time
		if stats.TotalBlockIOTime < 10 {
			continue
		}
		if count >= 25 {
			break
		}
		if _, ok := filteredByQueryFingerprint[stats.Fingerprint]; !ok {
			filteredByQueryFingerprint[stats.Fingerprint] = stats
			count += 1
		}
	}

	var filtered []*QueryStats
	for _, stats := range filteredByQueryFingerprint {
		filtered = append(filtered, stats)
	}

	return filtered
}
