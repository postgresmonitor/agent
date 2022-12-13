package db

import (
	"log"
	"postgres-monitor/logger"
	"strings"
	"time"
)

type MetadataMonitor struct {
	serverChannel chan *PostgresServer
}

func (m *MetadataMonitor) Run(postgresClient *PostgresClient) {
	if postgresClient.version == "" {
		postgresClient.version = m.FindPostgresVersion(postgresClient)
	}

	if postgresClient.maxConnections == 0 {
		postgresClient.maxConnections = m.FindMaxConnections(postgresClient)
	}

	if postgresClient.platform == "" {
		postgresClient.platform = GetPlatform(postgresClient)
	}

	server := &PostgresServer{
		ServerID: &ServerID{
			ConfigName:    postgresClient.serverID.ConfigName,
			ConfigVarName: postgresClient.serverID.ConfigVarName,
		},
		Platform:       postgresClient.platform,
		MaxConnections: postgresClient.maxConnections,
		Version:        postgresClient.version,
		MonitoredAt:    time.Now().UTC().Unix(),
	}
	if postgresClient.pgBouncerVersion != "" {
		server.PgBouncer = &PgBouncer{
			MaxServerConnections: postgresClient.pgBouncerMaxServerConnections,
			Version:              postgresClient.pgBouncerVersion,
		}
	}

	m.serverChannel <- server
}

func (m *MetadataMonitor) FindMaxConnections(postgresClient *PostgresClient) int64 {
	query := `select setting::int max_conn from pg_settings where name='max_connections'` + postgresMonitorQueryComment()

	var maxConnections int64

	err := postgresClient.client.QueryRow(query).Scan(&maxConnections)
	if err != nil {
		logger.Error("Max connection metrics error", "err", err)
		return 0
	}

	return maxConnections
}

func (m *MetadataMonitor) FindPostgresVersion(postgresClient *PostgresClient) string {
	var versionString string
	err := postgresClient.client.QueryRow("select current_setting('server_version') as version" + postgresMonitorQueryComment()).Scan(&versionString)
	if err != nil {
		log.Println("Error: ", err)
		return ""
	}

	// remove trailing version info - ex. 10.21 (Ubuntu 10.21-1.pgdg20.04+1)
	parts := strings.Split(versionString, " ")
	return parts[0]
}
