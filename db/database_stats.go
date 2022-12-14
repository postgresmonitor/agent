package db

import (
	"agent/logger"
	"agent/util"
	"database/sql"
	"sync"
	"time"
)

// stateful stats object that stores all pg_stat_database stats per database
// deltas are calculated each polling interval and reported as metrics
type DatabaseStatsState struct {
	// map of server config name + database to database stats
	Stats map[ServerID]*DatabaseStats
	mu    sync.Mutex
}

// https://www.postgresql.org/docs/10/monitoring-stats.html#PG-STAT-DATABASE-VIEW
// We calculate the stats delta between monitoring polls.
type DatabaseStats struct {
	CommittedTransactions  float64
	RollbackedTransactions float64
	BlocksRead             float64
	BlocksHit              float64
	BlocksHitPercent       float64
	BlockReadTime          float64
	BlockWriteTime         float64
	RowsReturned           float64
	RowsFetched            float64
	RowsInserted           float64
	RowsUpdated            float64
	RowsDeleted            float64
	Conflicts              float64
	TempFiles              float64
	TempBytes              float64
	Deadlocks              float64
}

// Calculate the delta between the last db stats and the latest db stats
func (d *DatabaseStats) Delta(latest *DatabaseStats) *DatabaseStats {
	stats := &DatabaseStats{
		CommittedTransactions:  latest.CommittedTransactions - d.CommittedTransactions,
		RollbackedTransactions: latest.RollbackedTransactions - d.RollbackedTransactions,
		BlocksRead:             latest.BlocksRead - d.BlocksRead,
		BlocksHit:              latest.BlocksHit - d.BlocksHit,
		BlockReadTime:          latest.BlockReadTime - d.BlockReadTime,
		BlockWriteTime:         latest.BlockWriteTime - d.BlockWriteTime,
		RowsReturned:           latest.RowsReturned - d.RowsReturned,
		RowsFetched:            latest.RowsFetched - d.RowsFetched,
		RowsInserted:           latest.RowsInserted - d.RowsInserted,
		RowsUpdated:            latest.RowsUpdated - d.RowsUpdated,
		RowsDeleted:            latest.RowsDeleted - d.RowsDeleted,
		Conflicts:              latest.Conflicts - d.Conflicts,
		TempFiles:              latest.TempFiles - d.TempFiles,
		TempBytes:              latest.TempBytes - d.TempBytes,
		Deadlocks:              latest.Deadlocks - d.Deadlocks,
	}
	stats.BlocksHitPercent = util.HitPercent(stats.BlocksHit, stats.BlocksRead)

	return stats
}

func (m *MetricMonitor) FindDatabaseStatMetrics(postgresClient *PostgresClient) []*Metric {
	// TODO: track newer fields in postgres 13 and 14
	query := `select xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted,
						tup_updated, tup_deleted, conflicts, temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time
						from pg_stat_database where datname = current_database()` + postgresMonitorQueryComment()

	now := time.Now().UTC().Unix()
	var dbStats DatabaseStats

	err := postgresClient.client.QueryRow(query).Scan(
		&dbStats.CommittedTransactions,
		&dbStats.RollbackedTransactions,
		&dbStats.BlocksRead,
		&dbStats.BlocksHit,
		&dbStats.RowsReturned,
		&dbStats.RowsFetched,
		&dbStats.RowsInserted,
		&dbStats.RowsUpdated,
		&dbStats.RowsDeleted,
		&dbStats.Conflicts,
		&dbStats.TempFiles,
		&dbStats.TempBytes,
		&dbStats.Deadlocks,
		&dbStats.BlockReadTime,
		&dbStats.BlockWriteTime,
	)
	if err != nil {
		logger.Error("DatabaseStat metrics error", "err", err)
		return []*Metric{}
	}

	// protect against concurrent map writes
	m.databaseStatsState.mu.Lock()
	defer m.databaseStatsState.mu.Unlock()

	if m.databaseStatsState.Stats == nil {
		m.databaseStatsState.Stats = make(map[ServerID]*DatabaseStats)
	}

	var delta *DatabaseStats

	previousStats, ok := m.databaseStatsState.Stats[*postgresClient.serverID]
	if !ok {
		// only report database stats once the stats object has a delta from two consecutive polls
		m.databaseStatsState.Stats[*postgresClient.serverID] = &dbStats
		return []*Metric{}
	}

	delta = previousStats.Delta(&dbStats)
	m.databaseStatsState.Stats[*postgresClient.serverID] = &dbStats

	entity := "database/" + postgresClient.serverID.Database

	return []*Metric{
		NewMetric("query.transactions.committed", delta.CommittedTransactions, entity, *postgresClient.serverID, now),
		NewMetric("query.transactions.rolledback", delta.RollbackedTransactions, entity, *postgresClient.serverID, now),
		NewMetric("query.rows.returned", delta.RowsReturned, entity, *postgresClient.serverID, now),
		NewMetric("query.rows.fetched", delta.RowsFetched, entity, *postgresClient.serverID, now),
		NewMetric("query.rows.inserted", delta.RowsInserted, entity, *postgresClient.serverID, now),
		NewMetric("query.rows.updated", delta.RowsUpdated, entity, *postgresClient.serverID, now),
		NewMetric("query.rows.deleted", delta.RowsDeleted, entity, *postgresClient.serverID, now),
		NewMetric("query.conflicts", delta.Conflicts, entity, *postgresClient.serverID, now),
		NewMetric("query.deadlocks", delta.Deadlocks, entity, *postgresClient.serverID, now),
		NewMetric("disk.temp.files", delta.TempFiles, entity, *postgresClient.serverID, now),
		NewMetric("disk.temp.bytes", delta.TempBytes, entity, *postgresClient.serverID, now),
		NewMetric("disk.io.blocks.read", delta.BlocksRead, entity, *postgresClient.serverID, now),
		NewMetric("disk.io.blocks.hit", delta.BlocksHit, entity, *postgresClient.serverID, now),
		NewMetric("disk.io.blocks.hit.percent", delta.BlocksHitPercent, entity, *postgresClient.serverID, now),
		NewMetric("disk.io.blocks.read.time", delta.BlockReadTime, entity, *postgresClient.serverID, now),
		NewMetric("disk.io.blocks.write.time", delta.BlockWriteTime, entity, *postgresClient.serverID, now),
	}
}

func (m *MetricMonitor) FindDatabaseCacheHitMetrics(postgresClient *PostgresClient) []*Metric {
	query := `select (sum(stati.idx_blks_hit)) / nullif(sum(stati.idx_blks_hit + stati.idx_blks_read),0) as index_cache_hit,
						sum(statt.heap_blks_hit) / nullif(sum(statt.heap_blks_hit) + sum(statt.heap_blks_read),0) as table_cache_hit
						from pg_statio_user_indexes stati, pg_statio_user_tables statt`

	var indexCacheHit sql.NullFloat64
	var tableCacheHit sql.NullFloat64

	err := postgresClient.client.QueryRow(query).Scan(&indexCacheHit, &tableCacheHit)
	if err != nil {
		logger.Error("Database cache hit metrics error", "err", err)
		return []*Metric{}
	}

	now := time.Now().UTC().Unix()

	var metrics []*Metric

	// attach cache hit rate metrics to server
	if indexCacheHit.Valid {
		metrics = append(metrics, NewMetric("cache.index.hit.rate", indexCacheHit.Float64, "", *postgresClient.serverID, now))
	}
	if tableCacheHit.Valid {
		metrics = append(metrics, NewMetric("cache.table.hit.rate", tableCacheHit.Float64, "", *postgresClient.serverID, now))
	}

	return metrics
}
