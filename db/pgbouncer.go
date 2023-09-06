package db

import (
	"agent/logger"
	"agent/util"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

// stateful stats object that stores all pgbouncer stats per database
// deltas are calculated each polling interval and reported as metrics
type PgBouncerStatsState struct {
	// map of server config name + database to database stats
	Stats map[ServerID]*PgBouncerStats
	mu    sync.Mutex
}

type PgBouncer struct {
	MaxServerConnections int64
	Version              string
}

type PgBouncerStats struct {
	Database         string
	TransactionCount float64
	QueryCount       float64
	BytesReceived    float64
	BytesSent        float64
	TransactionTime  float64
	QueryTime        float64
	WaitTime         float64
}

type PgBouncerPoolStats struct {
	ClientActiveConnections  float64
	ClientWaitingConnections float64
	ServerActiveConnections  float64
	ServerIdleConnections    float64
	MaxWaitTime              float64
}

func (s *PgBouncerStats) Delta(latest *PgBouncerStats) *PgBouncerStats {
	return &PgBouncerStats{
		TransactionCount: latest.TransactionCount - s.TransactionCount,
		QueryCount:       latest.QueryCount - s.QueryCount,
		BytesReceived:    latest.BytesReceived - s.BytesReceived,
		BytesSent:        latest.BytesSent - s.BytesSent,
		TransactionTime:  latest.TransactionTime - s.TransactionTime,
		QueryTime:        latest.QueryTime - s.QueryTime,
		WaitTime:         latest.WaitTime - s.WaitTime,
	}
}

type PgBouncerMonitor struct {
	pgBouncerStatsState *PgBouncerStatsState
	metricsChannel      chan []*Metric
}

func (m *PgBouncerMonitor) Run(postgresClient *PostgresClient) {
	if postgresClient.pgBouncerEnabled == nil {
		m.InitPgBouncerMetadata(postgresClient)
	}

	if *postgresClient.pgBouncerEnabled {
		metrics := m.GetMetrics(postgresClient)

		select {
		case m.metricsChannel <- metrics:
			// sent
		default:
			logger.Warn("Dropping metrics: channel buffer full")
		}
	}
}

func (m *PgBouncerMonitor) InitPgBouncerMetadata(postgresClient *PostgresClient) {
	enabled := false

	// pgbouncer monitoring is not supported for aws dbs
	if postgresClient.isAuroraPlatform || postgresClient.isRDSPlatform {
		postgresClient.SetPgBouncerEnabled(false)
		return
	}

	pgBouncerConn := m.GetConnection(postgresClient)
	if pgBouncerConn != nil {
		defer pgBouncerConn.Close()

		version := m.GetVersion(pgBouncerConn)
		if version != "" {

			// if the pgbouncer pool has more than 0 client or server connections going through the pool
			// then treat pgbouncer as enabled for this server
			// heroku runs pgbouncer by default regardless if an app is using pgbouncer to connect to their
			// database - this isn't a full proof solution on heroku since a small number of connections are used periodically
			// likely by heroku health check services
			poolStats := m.GetPoolStats(pgBouncerConn, postgresClient.serverID.Database)
			if poolStats != nil {
				if poolStats.ClientActiveConnections > 0 || poolStats.ClientWaitingConnections > 0 || poolStats.ServerActiveConnections > 0 {
					enabled = true

					maxServerConnections := m.GetMaxServerConnections(pgBouncerConn, postgresClient.serverID.Database)

					postgresClient.pgBouncerVersion = version
					postgresClient.pgBouncerMaxServerConnections = maxServerConnections

					// print version to help with debugging future pgbouncer issues
					logger.Info("PgBouncer", "server", postgresClient.serverID.Name, "enabled", enabled, "version", version)
				}
			}
		}
	}

	postgresClient.SetPgBouncerEnabled(enabled)
}

// we could also use show clients; and show servers; to get additional info about connections
func (m *PgBouncerMonitor) GetMetrics(postgresClient *PostgresClient) []*Metric {
	var metrics []*Metric

	// protect against concurrent map writes
	m.pgBouncerStatsState.mu.Lock()
	defer m.pgBouncerStatsState.mu.Unlock()

	if m.pgBouncerStatsState.Stats == nil {
		m.pgBouncerStatsState.Stats = make(map[ServerID]*PgBouncerStats)
	}

	now := time.Now().UTC().Unix()

	pgBouncerConn := m.GetConnection(postgresClient)
	if pgBouncerConn == nil {
		return []*Metric{}
	}
	defer pgBouncerConn.Close()

	// total stats
	pgBouncerStats := m.GetTotalStats(pgBouncerConn, postgresClient.serverID.Database)
	previousStats, ok := m.pgBouncerStatsState.Stats[*postgresClient.serverID]
	if ok {
		// only report pgbouncers total stats once the stats object has a delta from two consecutive polls
		delta := previousStats.Delta(pgBouncerStats)
		transactionTimeAvg := util.Percent(delta.TransactionTime, delta.TransactionCount)
		queryTimeAvg := util.Percent(delta.QueryTime, delta.QueryCount)
		metrics = append(metrics,
			NewMetric("pgbouncer.transactions", delta.TransactionCount, "", *postgresClient.serverID, now),
			NewMetric("pgbouncer.queries", delta.QueryCount, "", *postgresClient.serverID, now),
			NewMetric("pgbouncer.received.bytes", delta.BytesReceived, "", *postgresClient.serverID, now),
			NewMetric("pgbouncer.sent.bytes", delta.BytesSent, "", *postgresClient.serverID, now),
			NewMetric("pgbouncer.transactions.time", delta.TransactionTime, "", *postgresClient.serverID, now),
			NewMetric("pgbouncer.transactions.time.avg", transactionTimeAvg, "", *postgresClient.serverID, now),
			NewMetric("pgbouncer.queries.time", delta.QueryTime, "", *postgresClient.serverID, now),
			NewMetric("pgbouncer.queries.time.avg", queryTimeAvg, "", *postgresClient.serverID, now),
			NewMetric("pgbouncer.wait.time", delta.WaitTime, "", *postgresClient.serverID, now),
		)
	}
	m.pgBouncerStatsState.Stats[*postgresClient.serverID] = pgBouncerStats

	// pool stats
	poolStats := m.GetPoolStats(pgBouncerConn, postgresClient.serverID.Database)

	metrics = append(metrics,
		NewMetric("pgbouncer.connections.client.active", poolStats.ClientActiveConnections, "", *postgresClient.serverID, now),
		NewMetric("pgbouncer.connections.client.waiting", poolStats.ClientWaitingConnections, "", *postgresClient.serverID, now),
		NewMetric("pgbouncer.connections.server.active", poolStats.ServerActiveConnections, "", *postgresClient.serverID, now),
		NewMetric("pgbouncer.connections.server.idle", poolStats.ServerIdleConnections, "", *postgresClient.serverID, now),
		NewMetric("pgbouncer.wait.time.max", poolStats.MaxWaitTime, "", *postgresClient.serverID, now),
	)

	return metrics
}

func (m *PgBouncerMonitor) GetTotalStats(pgBouncerConn *sql.DB, database string) *PgBouncerStats {
	var pgBouncerStats *PgBouncerStats

	// only record total stats and not averages
	rows, err := pgBouncerConn.Query("show stats_totals")

	if err != nil {
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var stats PgBouncerStats

		err := rows.Scan(
			&stats.Database,
			&stats.TransactionCount,
			&stats.QueryCount,
			&stats.BytesReceived,
			&stats.BytesSent,
			&stats.TransactionTime,
			&stats.QueryTime,
			&stats.WaitTime,
		)
		if err != nil {
			continue
		}

		// convert microseconds to milliseconds
		stats.TransactionTime /= 1000
		stats.QueryTime /= 1000
		stats.WaitTime /= 1000

		if stats.Database == database {
			pgBouncerStats = &stats
		}
	}

	return pgBouncerStats
}

func (m *PgBouncerMonitor) GetPoolStats(pgBouncerConn *sql.DB, database string) *PgBouncerPoolStats {
	var poolStats *PgBouncerPoolStats

	rows, err := pgBouncerConn.Query("show pools")

	if err != nil {
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var stats PgBouncerPoolStats

		// unused binding params that are needed for the query results
		var db string
		var user string
		var serverUsedConnections int64
		var serverTestedConnections int64
		var serverLoginConnections int64
		var maxWaitMicroseconds int64
		var poolMode string

		err := rows.Scan(
			&db,
			&user,
			&stats.ClientActiveConnections,
			&stats.ClientWaitingConnections,
			&stats.ServerActiveConnections,
			&stats.ServerIdleConnections,
			&serverUsedConnections,
			&serverTestedConnections,
			&serverLoginConnections,
			&stats.MaxWaitTime,
			&maxWaitMicroseconds,
			&poolMode,
		)
		if err != nil {
			fmt.Printf("PgBouncer pool stats error %v", err)
			continue
		}

		if db == database {
			poolStats = &stats
		}
	}

	return poolStats
}

func (m *PgBouncerMonitor) GetMaxServerConnections(pgBouncerConn *sql.DB, database string) int64 {
	var maxServerConnections int64

	rows, err := pgBouncerConn.Query("show databases")

	if err != nil {
		return 0
	}
	defer rows.Close()

	for rows.Next() {
		var max int64

		// unused binding params that are needed for the query results
		var name sql.NullString
		var host sql.NullString
		var port int64
		var db string
		var user sql.NullString
		var poolSize int64
		var reservePool int64
		var poolMode sql.NullString
		var current int64
		var paused int64
		var disabled int64

		err := rows.Scan(
			&name,
			&host,
			&port,
			&db,
			&user,
			&poolSize,
			&reservePool,
			&poolMode,
			&max,
			&current,
			&paused,
			&disabled,
		)
		if err != nil {
			fmt.Printf("PgBouncer db stats error %v", err)
			continue
		}

		if db == database {
			maxServerConnections = max
		}
	}

	return maxServerConnections
}

func (m *PgBouncerMonitor) GetVersion(pgBouncerConn *sql.DB) string {
	var version string

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pgBouncerConn.QueryRowContext(ctx, "show version").Scan(&version)
	if err != nil {
		return ""
	}

	// remove leading PgBouncer to just get the version
	version = strings.TrimSpace(strings.Replace(version, "PgBouncer ", "", -1))

	return version
}

func (m *PgBouncerMonitor) GetConnection(postgresClient *PostgresClient) *sql.DB {
	url := m.BuildPgBouncerURL(postgresClient.url)
	if url == "" {
		return nil
	}

	// don't test the connection with a ping request since that errors for pgbouncer
	return NewConn(url, false)
}

// replace port and database with 5433 and pgbouncer
func (m *PgBouncerMonitor) BuildPgBouncerURL(url string) string {
	parts := strings.Split(url, ":")

	if len(parts) < 2 {
		return ""
	}

	// set application name for db connections and don't use prepared statements since pgbouncer doesn't support it
	return fmt.Sprintf("%s:%s:%s:%s", parts[0], parts[1], parts[2], "5433/pgbouncer?application_name=postgres-monitor-agent&statement_cache_mode=describe&prefer_simple_protocol=true")
}
