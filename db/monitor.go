package db

import (
	"agent/config"
	"agent/errors"
	"agent/logger"
	"agent/util"
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
	defer errors.DeferRecoverWithCallback(func(err error) {
		msg := reflect.TypeOf(m.monitor).Elem().Name() + " panicked!"
		logger.Error(msg, "server", m.postgresClient.serverID.Name, "panic", err.Error())
	})

	started_at := time.Now().UTC().UnixNano()

	m.monitor.Run(m.postgresClient)

	duration := NanosecondsToMilliseconds(time.Now().UTC().UnixNano() - started_at)
	if m.config.IsLogDebug() {
		msg := reflect.TypeOf(m.monitor).Elem().Name() + " ran"
		logger.Debug(msg, "server", m.postgresClient.serverID.Name, "duration_ms", duration)
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
