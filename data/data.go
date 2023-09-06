package data

import (
	"agent/db"
	"agent/errors"
	"reflect"
	"sync"
)

// metric values are always strings as a lowest-common-denominator
type LogMetrics map[string]string

type Data struct {
	LogMetrics               []LogMetrics
	Metrics                  []db.Metric
	PostgresServers          []db.PostgresServer
	Databases                []db.Database
	Replications             []db.Replication
	Settings                 []db.Setting
	QueryStats               []db.QueryStats
	Errors                   []errors.ErrorReport
	LogTestMessageReceivedAt int64
	mu                       sync.Mutex
}

func (d *Data) AddLogTestMessageReceivedAt(receivedAt int64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.LogTestMessageReceivedAt = receivedAt
}

func (d *Data) AddLogMetrics(newLogMetrics map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.LogMetrics = append(d.LogMetrics, newLogMetrics)
}

func (d *Data) AddMetrics(metrics []*db.Metric) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// just append new metric records - we'll aggregate them in request
	for _, metric := range metrics {
		d.Metrics = append(d.Metrics, *metric)
	}
}

func (d *Data) AddPostgresServer(newServer *db.PostgresServer) {
	d.mu.Lock()
	defer d.mu.Unlock()

	existingIndex := -1

	// find existing postgres server
	for index, server := range d.PostgresServers {
		// uniqueness by config name and config var name (ex. GREEN and GREEN_URL)
		if server.ServerID.Name == newServer.ServerID.Name && server.ServerID.ConfigVarName == newServer.ServerID.ConfigVarName {
			existingIndex = index
			break
		}
	}

	// new server so just append and return
	if existingIndex == -1 {
		d.PostgresServers = append(d.PostgresServers, *newServer)
		return
	}

	// add new server data to existing server
	// always update monitored at for the latest
	d.PostgresServers[existingIndex].MonitoredAt = newServer.MonitoredAt

	if newServer.Version != "" {
		d.PostgresServers[existingIndex].Version = newServer.Version
	}

	if newServer.MaxConnections != 0 {
		d.PostgresServers[existingIndex].MaxConnections = newServer.MaxConnections
	}
}

func (d *Data) AddDatabase(database *db.Database) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// find database and replace it since we only need the latest schema per db
	existingIndex := -1

	for index, existingDatabase := range d.Databases {
		// uniqueness by server id (config name & database name)
		if reflect.DeepEqual(existingDatabase.ServerID, database.ServerID) {
			existingIndex = index
			break
		}
	}

	if existingIndex == -1 {
		d.Databases = append(d.Databases, *database)
	} else {
		// replace existing
		d.Databases[existingIndex] = *database
	}
}

func (d *Data) AddReplication(replication *db.Replication) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// find replica and replicas and replace them since we only need the latest state
	existingIndex := -1

	for index, existingReplication := range d.Replications {
		// uniqueness by server id (config id & database name)
		if reflect.DeepEqual(existingReplication.ServerID, replication.ServerID) {
			existingIndex = index
			break
		}
	}

	if existingIndex == -1 {
		d.Replications = append(d.Replications, *replication)
	} else {
		// replace existing
		d.Replications[existingIndex] = *replication
	}
}

func (d *Data) AddSettings(settings []*db.Setting) {
	for _, setting := range settings {
		d.AddSetting(setting)
	}
}

func (d *Data) AddSetting(setting *db.Setting) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// find setting and replace it since we only need the latest setting per db
	existingIndex := -1

	for index, existingSetting := range d.Settings {
		// uniqueness by setting name and server id
		if existingSetting.Name == setting.Name && reflect.DeepEqual(existingSetting.ServerID, setting.ServerID) {
			existingIndex = index
			break
		}
	}

	if existingIndex == -1 {
		d.Settings = append(d.Settings, *setting)
	} else {
		// replace existing
		d.Settings[existingIndex] = *setting
	}
}

func (d *Data) AddQueryStats(stats []*db.QueryStats) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// append stats
	for _, stat := range stats {
		d.QueryStats = append(d.QueryStats, *stat)
	}
}

func (d *Data) AddErrorReport(err *errors.ErrorReport) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// track the first 10 errors
	if len(d.Errors) < 10 {
		d.Errors = append(d.Errors, *err)
	} else if err.Panic {
		// if a panic is reported, always track it
		// unless at 10 errors already and a panic has been tracked
		var panicAlreadyTracked bool
		for _, err := range d.Errors {
			if err.Panic {
				panicAlreadyTracked = true
				break
			}
		}

		if !panicAlreadyTracked {
			d.Errors = append(d.Errors, *err)
		}
	}
}

func (d *Data) CopyAndReset() *Data {
	d.mu.Lock()
	defer d.mu.Unlock()

	logMetricsCopy := make([]LogMetrics, len(d.LogMetrics))
	copy(logMetricsCopy, d.LogMetrics)

	metricsCopy := make([]db.Metric, len(d.Metrics))
	copy(metricsCopy, d.Metrics)

	postgresServersCopy := make([]db.PostgresServer, len(d.PostgresServers))
	copy(postgresServersCopy, d.PostgresServers)

	databasesCopy := make([]db.Database, len(d.Databases))
	copy(databasesCopy, d.Databases)

	replicationsCopy := make([]db.Replication, len(d.Replications))
	copy(replicationsCopy, d.Replications)

	settingsCopy := make([]db.Setting, len(d.Settings))
	copy(settingsCopy, d.Settings)

	queryStatsCopy := make([]db.QueryStats, len(d.QueryStats))
	copy(queryStatsCopy, d.QueryStats)

	errorsCopy := make([]errors.ErrorReport, len(d.Errors))
	copy(errorsCopy, d.Errors)

	copiedData := &Data{
		LogMetrics:               logMetricsCopy,
		Metrics:                  metricsCopy,
		PostgresServers:          postgresServersCopy,
		Databases:                databasesCopy,
		Replications:             replicationsCopy,
		Settings:                 settingsCopy,
		QueryStats:               queryStatsCopy,
		Errors:                   errorsCopy,
		LogTestMessageReceivedAt: d.LogTestMessageReceivedAt,
	}

	d.LogMetrics = []LogMetrics{}
	d.Metrics = []db.Metric{}
	d.PostgresServers = []db.PostgresServer{}
	d.Databases = []db.Database{}
	d.Replications = []db.Replication{}
	d.Settings = []db.Setting{}
	d.QueryStats = []db.QueryStats{}
	d.Errors = []errors.ErrorReport{}
	d.LogTestMessageReceivedAt = 0

	return copiedData
}
