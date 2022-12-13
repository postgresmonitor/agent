package db

import (
	"postgres-monitor/logger"
	"time"
)

type Metric struct {
	Name       string
	Value      float64
	ServerID   ServerID
	Entity     string
	MeasuredAt int64
}

func NewMetric(name string, value float64, entity string, serverID ServerID, measuredAt int64) *Metric {
	return &Metric{
		Name:       name,
		Value:      value,
		Entity:     entity,
		ServerID:   serverID,
		MeasuredAt: measuredAt,
	}
}

type MetricMonitor struct {
	metricsChannel     chan *Metric
	databaseStatsState *DatabaseStatsState
}

func (m *MetricMonitor) Run(postgresClient *PostgresClient) {
	metrics := m.FindUsedConnectionsMetric(postgresClient)
	metrics = append(metrics, m.FindDatabaseStatMetrics(postgresClient)...)
	metrics = append(metrics, m.FindDatabaseCacheHitMetrics(postgresClient)...)

	for _, metric := range metrics {
		m.metricsChannel <- metric
	}
}

func (m *MetricMonitor) FindUsedConnectionsMetric(postgresClient *PostgresClient) []*Metric {
	// used connections include active connections and reserved connections
	query := `select used, reserved from
						(select count(*) used from pg_stat_activity) q1,
						(select setting::int reserved from pg_settings where name='superuser_reserved_connections') q2` + postgresMonitorQueryComment()

	now := time.Now().UTC().Unix()
	var used float64
	var reserved float64

	err := postgresClient.client.QueryRow(query).Scan(&used, &reserved)
	if err != nil {
		logger.Error("Connection metrics error", "err", err)
		return []*Metric{}
	}

	return []*Metric{
		NewMetric("connections.used", used, "", *postgresClient.serverID, now),
		NewMetric("connections.reserved", reserved, "", *postgresClient.serverID, now),
	}
}
