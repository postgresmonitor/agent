package db

import (
	"postgres-monitor/config"
	"postgres-monitor/logger"
	"postgres-monitor/util"
	"reflect"
	"time"
)

type Monitor interface {
	Run(postgresClient *PostgresClient)
}

type MonitorWorker struct {
	config         config.Config
	monitor        Monitor
	postgresClient *PostgresClient
}

func (m *MonitorWorker) Start() {
	// recover from monitor panics but log what happened
	defer func() {
		if p := recover(); p != nil {
			msg := reflect.TypeOf(m.monitor).Elem().Name() + " panicked!"
			logger.Error(msg, "server", m.postgresClient.serverID.ConfigName, "panic", p)
		}
	}()

	started_at := time.Now().UTC().UnixNano()

	m.monitor.Run(m.postgresClient)

	duration := NanosecondsToMilliseconds(time.Now().UTC().UnixNano() - started_at)
	if m.config.IsLogDebug() {
		msg := reflect.TypeOf(m.monitor).Elem().Name() + " ran"
		logger.Debug(msg, "server", m.postgresClient.serverID.ConfigName, "duration_ms", duration)
	}
}

func NewMonitorWorker(config config.Config, postgresClient *PostgresClient, monitor Monitor) *MonitorWorker {
	return &MonitorWorker{
		monitor:        monitor,
		config:         config,
		postgresClient: postgresClient,
	}
}

func NanosecondsToMilliseconds(duration int64) float64 {
	durationMs := float64(duration) / float64(1000000) // ns to ms
	durationMs = util.Round(durationMs)
	return durationMs
}
