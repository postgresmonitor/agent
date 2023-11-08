package db

import (
	"agent/errors"
	"agent/logger"
	"log"
	"strings"
	"time"
)

type MetadataMonitor struct {
	dataChannel chan interface{}
}

func (m *MetadataMonitor) Run(postgresClient *PostgresClient) {
	if postgresClient.version == "" {
		postgresClient.version = m.FindPostgresVersion(postgresClient)
	}

	if postgresClient.maxConnections == 0 {
		postgresClient.maxConnections = m.FindMaxConnections(postgresClient)
	}

	server := &PostgresServer{
		ServerID: &ServerID{
			Name:          postgresClient.serverID.Name,
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

	select {
	case m.dataChannel <- server:
		// sent
	default:
		logger.Warn("Dropping server: channel buffer full")
	}
}

func (m *MetadataMonitor) FindMaxConnections(postgresClient *PostgresClient) int64 {
	query := `select setting::int max_conn from pg_settings where name='max_connections'` + postgresMonitorQueryComment()

	var maxConnections int64

	err := postgresClient.client.QueryRow(query).Scan(&maxConnections)
	if err != nil {
		logger.Error("Max connection metrics error", "err", err)
		errors.Report(err)
		return 0
	}

	return maxConnections
}

func (m *MetadataMonitor) FindPostgresVersion(postgresClient *PostgresClient) string {
	var versionString string

	if postgresClient.isAuroraPlatform {
		versionString = m.FindAuroraPostgresVersion(postgresClient)
	}

	if versionString == "" {
		err := postgresClient.client.QueryRow("select current_setting('server_version') as version" + postgresMonitorQueryComment()).Scan(&versionString)
		if err != nil {
			log.Println("Error: ", err)
			return ""
		}
	}

	// remove trailing version info - ex. 10.21 (Ubuntu 10.21-1.pgdg20.04+1)
	parts := strings.Split(versionString, " ")
	return parts[0]
}

func (m *MetadataMonitor) FindAuroraPostgresVersion(postgresClient *PostgresClient) string {
	var versionString string

	// the aurora_version() method may provide a more specific version number with patch version
	err := postgresClient.client.QueryRow("select * from aurora_version()" + postgresMonitorQueryComment()).Scan(&versionString)
	if err != nil {
		log.Println("Error: ", err)
		return ""
	}

	return versionString
}
