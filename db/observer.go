package db

import (
	"agent/config"
	"agent/errors"
	"agent/logger"
	"agent/schedule"
	"time"
)

type Observer struct {
	config                 config.Config
	startLogsServerChannel chan bool
	dataChannel            chan interface{}
	rawSlowQueryChannel    chan *SlowQuery

	// stateful stats for the life of the observer
	databaseSchemaState *DatabaseSchemaState
	databaseStatsState  *DatabaseStatsState
	pgBouncerStatsState *PgBouncerStatsState
	queryStatsState     *QueryStatsState

	explainer  *Explainer
	obfuscator *Obfuscator

	postgresClients []*PostgresClient

	startedLogServer bool
}

type ServerID struct {
	// ex. GREEN
	Name string

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

type RDSInstanceFoundEvent struct {
	InstanceID string
	IsAurora   bool
}

// Creates a new DB observer using the present config env vars
func NewObserver(config config.Config, dataChannel chan interface{}, startLogsServerChannel chan bool, rawSlowQueryChannel chan *SlowQuery, awsInstanceDiscoveredChannel chan *RDSInstanceFoundEvent) *Observer {
	postgresClients := BuildPostgresClients(config)

	if len(postgresClients) == 0 {
		logger.Error("No Postgres servers were found")
	} else {
		for _, client := range postgresClients {
			logger.Info("Monitoring Postgres server", "name", client.serverID.Name, "platform", client.platform)

			// notify cloudwatch observer
			if config.MonitorCloudwatchMetrics && (client.isAuroraPlatform || client.isRDSPlatform) {
				awsInstanceDiscoveredChannel <- &RDSInstanceFoundEvent{
					InstanceID: client.serverID.Name,
					IsAurora:   client.isAuroraPlatform,
				}
			}
		}
	}

	return &Observer{
		config:                 config,
		dataChannel:            dataChannel,
		startLogsServerChannel: startLogsServerChannel,
		rawSlowQueryChannel:    rawSlowQueryChannel,
		databaseSchemaState:    &DatabaseSchemaState{},
		databaseStatsState:     &DatabaseStatsState{},
		pgBouncerStatsState:    &PgBouncerStatsState{},
		queryStatsState:        &QueryStatsState{},
		explainer:              &Explainer{},
		obfuscator:             &Obfuscator{},
		postgresClients:        postgresClients,
		startedLogServer:       false,
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
		errors.Report(err)
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

	// sleep so the main thread doesn't exit
	time.Sleep(1 * time.Hour * 24 * 30 * 12)
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
					dataChannel:         o.dataChannel,
				},
			).Start()
		}

		// monitor metadata next to ensure version and other high level state is set
		NewMonitorWorker(
			o.config,
			postgresClient,
			&MetadataMonitor{
				dataChannel: o.dataChannel,
			},
		).Start()

		// if the current platform requires a log server then start one
		if !o.startedLogServer && PlatformRequiresLogServer(postgresClient.platform) {
			o.startLogsServerChannel <- true
			o.startedLogServer = true
		}

		// bootstrap schema as well to ensure that we have delta metrics
		// set up correctly for database schemas with two polling intervals
		if o.config.MonitorSchema {
			NewMonitorWorker(
				o.config,
				postgresClient,
				&SchemaMonitor{
					dataChannel:         o.dataChannel,
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
				dataChannel: o.dataChannel,
			},
		).Start()

		if o.config.MonitorPgBouncer {
			go NewMonitorWorker(
				o.config,
				postgresClient,
				&PgBouncerMonitor{
					pgBouncerStatsState: o.pgBouncerStatsState,
					dataChannel:         o.dataChannel,
				},
			).Start()
		}

		if o.config.MonitorReplication {
			go NewMonitorWorker(
				o.config,
				postgresClient,
				&ReplicationMonitor{
					dataChannel:     o.dataChannel,
					postgresClients: o.postgresClients,
				},
			).Start()
		}

		go NewMonitorWorker(
			o.config,
			postgresClient,
			&MetricMonitor{
				dataChannel:        o.dataChannel,
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
				dataChannel:         o.dataChannel,
				obfuscator:          o.obfuscator,
				monitorAgentQueries: o.config.MonitorAgentQueries,
			},
		).Start()
	}
}
