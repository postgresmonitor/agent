package db

import (
	"agent/config"
	"agent/logger"
	"agent/schedule"
)

type Observer struct {
	config              config.Config
	serverChannel       chan *PostgresServer
	schemaChannel       chan *Database
	settingsChannel     chan []*Setting
	metricsChannel      chan []*Metric
	queryStatsChannel   chan []*QueryStats
	replicationChannel  chan *Replication
	rawSlowQueryChannel chan *SlowQuery

	// stateful stats for the life of the observer
	databaseSchemaState *DatabaseSchemaState
	databaseStatsState  *DatabaseStatsState
	pgBouncerStatsState *PgBouncerStatsState
	queryStatsState     *QueryStatsState

	explainer  *Explainer
	obfuscator *Obfuscator

	postgresClients []*PostgresClient
}

type ServerID struct {
	// ex. GREEN
	ConfigName string

	// ex. GREEN_URL
	ConfigVarName string

	// db name
	Database string
}

type PostgresServer struct {
	ServerID *ServerID

	Platform       string
	MaxConnections int64
	PgBouncer      *PgBouncer
	Version        string
	MonitoredAt    int64
}

// Creates a new DB observer using the present config env vars
func NewObserver(config config.Config, serverChannel chan *PostgresServer, schemaChannel chan *Database, replicationChannel chan *Replication, metricsChannel chan []*Metric, queryStatsChannel chan []*QueryStats, settingsChannel chan []*Setting, rawSlowQueryChannel chan *SlowQuery) *Observer {
	postgresClients := BuildPostgresClients(config)

	if len(postgresClients) == 0 {
		logger.Error("No Postgres servers were found")
	} else {
		for _, client := range postgresClients {
			logger.Info("Monitoring Postgres server", "configName", client.serverID.ConfigName)
		}
	}

	return &Observer{
		config:              config,
		serverChannel:       serverChannel,
		schemaChannel:       schemaChannel,
		settingsChannel:     settingsChannel,
		replicationChannel:  replicationChannel,
		queryStatsChannel:   queryStatsChannel,
		metricsChannel:      metricsChannel,
		rawSlowQueryChannel: rawSlowQueryChannel,
		databaseSchemaState: &DatabaseSchemaState{},
		databaseStatsState:  &DatabaseStatsState{},
		pgBouncerStatsState: &PgBouncerStatsState{},
		queryStatsState:     &QueryStatsState{},
		explainer:           &Explainer{},
		obfuscator:          &Obfuscator{},
		postgresClients:     postgresClients,
	}
}

func (o *Observer) WriteLogTestMessage() {
	// raise test log error message for the first attached database
	if len(o.postgresClients) == 0 {
		logger.Warn("No postgres databases to write log test message to")
		return
	}

	postgresClient := o.postgresClients[0]

	// https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
	testMessage := `DO $$ BEGIN RAISE NOTICE 'POSTGRES_MONITOR_AGENT_TEST'; END $$;`

	err := postgresClient.client.Exec(testMessage)
	if err != nil {
		logger.Error("Error writing log test message with RAISE NOTICE", "err", err)
	}
}

// Starts go routines to monitor schema and stats
func (o *Observer) Start() {
	o.BootstrapMetatdataAndSchemas()

	go schedule.ScheduleAndRunNow(o.Monitor, o.config.MonitorInterval)

	if o.config.MonitorSchema {
		go schedule.ScheduleAndRunNow(o.MonitorSchemas, o.config.MonitorSchemaInterval)
	}

	if o.config.MonitorSettings {
		go schedule.ScheduleAndRunNow(o.MonitorSettings, o.config.MonitorSettingsInterval)
	}

	if o.config.MonitorQueryStats {
		go schedule.ScheduleAndRunNow(o.MonitorQueryStats, o.config.MonitorQueryStatsInterval)
	}

	go o.MonitorSlowQueries()
}

func (o *Observer) BootstrapMetatdataAndSchemas() {
	for _, postgresClient := range o.postgresClients {
		// monitor pgbouncer first to make sure pgbouncer version is set on the client
		if o.config.MonitorPgBouncer {
			NewMonitorWorker(
				o.config,
				postgresClient,
				&PgBouncerMonitor{
					pgBouncerStatsState: o.pgBouncerStatsState,
					metricsChannel:      o.metricsChannel,
				},
			).Start()
		}

		// monitor metadata next to ensure version and other high level state is set
		NewMonitorWorker(
			o.config,
			postgresClient,
			&MetadataMonitor{
				serverChannel: o.serverChannel,
			},
		).Start()

		// bootstrap schema as well to ensure that we have delta metrics
		// set up correctly for database schemas with two polling intervals
		if o.config.MonitorSchema {
			NewMonitorWorker(
				o.config,
				postgresClient,
				&SchemaMonitor{
					schemaChannel:       o.schemaChannel,
					databaseSchemaState: o.databaseSchemaState,
				},
			).Start()
		}
	}
}

func (o *Observer) Monitor() {
	for _, postgresClient := range o.postgresClients {
		go NewMonitorWorker(
			o.config,
			postgresClient,
			&MetadataMonitor{
				serverChannel: o.serverChannel,
			},
		).Start()

		if o.config.MonitorPgBouncer {
			go NewMonitorWorker(
				o.config,
				postgresClient,
				&PgBouncerMonitor{
					pgBouncerStatsState: o.pgBouncerStatsState,
					metricsChannel:      o.metricsChannel,
				},
			).Start()
		}

		if o.config.MonitorReplication {
			go NewMonitorWorker(
				o.config,
				postgresClient,
				&ReplicationMonitor{
					replicationChannel: o.replicationChannel,
					metricsChannel:     o.metricsChannel,
					postgresClients:    o.postgresClients,
				},
			).Start()
		}

		go NewMonitorWorker(
			o.config,
			postgresClient,
			&MetricMonitor{
				metricsChannel:     o.metricsChannel,
				databaseStatsState: o.databaseStatsState,
			},
		).Start()
	}
}

func (o *Observer) MonitorQueryStats() {
	for _, postgresClient := range o.postgresClients {
		go NewMonitorWorker(
			o.config,
			postgresClient,
			&QueryStatsMonitor{
				queryStatsState:     o.queryStatsState,
				queryStatsChannel:   o.queryStatsChannel,
				obfuscator:          o.obfuscator,
				monitorAgentQueries: o.config.MonitorAgentQueries,
			},
		).Start()
	}
}
