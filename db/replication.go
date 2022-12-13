package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgtype"
)

type Replication struct {
	ServerID *ServerID

	// populated if current server is a replica
	Replica *Replica
	// following replicas
	Replicas []*ReplicaClient
}

// populated on a replica
type Replica struct {
	ApplicationName   string // application_name:backend_start
	PrimaryHost       string
	PrimaryConfigName string // ex GREEN
	Status            string
	Lag               pgtype.Interval
	MeasuredAt        int64
}

type ReplicaClient struct {
	ApplicationName string // application_name:backend_start
	ClientAddr      sql.NullString
	ClientHostname  sql.NullString
	ClientPort      sql.NullInt32
	BackendStart    sql.NullTime
	Backendxmin     sql.NullInt64
	State           sql.NullString
	WriteLag        pgtype.Interval
	FlushLag        pgtype.Interval
	ReplayLag       pgtype.Interval
	WriteLagBytes   float64
	FlushLagBytes   float64
	ReplayLagBytes  float64
	SyncPriority    sql.NullInt32
	SyncState       sql.NullString
	MeasuredAt      int64
}

type ReplicationMonitor struct {
	replicationChannel chan *Replication
	metricsChannel     chan *Metric
	postgresClients    []*PostgresClient
}

func (m *ReplicationMonitor) Run(postgresClient *PostgresClient) {
	replica := m.FindReplica(postgresClient)
	replicas := m.FindReplicas(postgresClient)

	replication := &Replication{
		ServerID: postgresClient.serverID,
		Replica:  replica,
		Replicas: replicas,
	}

	m.replicationChannel <- replication

	m.ReportReplicationLagMetrics(postgresClient.serverID, replica, replicas)
}

func (m *ReplicationMonitor) ReportReplicationLagMetrics(serverID *ServerID, replica *Replica, replicaClients []*ReplicaClient) {
	// send lag metrics
	if replica != nil && isPgtypePresent(replica.Lag.Status) {
		// sent if server is a replica
		m.metricsChannel <- NewMetric(
			"replication.standby.lag.local.ms",
			microToMilliseconds(replica.Lag.Microseconds),
			"replica/standby/"+replica.ApplicationName,
			*serverID,
			replica.MeasuredAt,
		)
	}

	// sent for all replicas of current server
	for _, replicaClient := range replicaClients {
		if replicaClient == nil {
			continue
		}

		entity := "replica/standby/" + replicaClient.ApplicationName

		// ms lag
		if isPgtypePresent(replicaClient.WriteLag.Status) {
			m.metricsChannel <- NewMetric(
				"replication.standby.lag.write.ms",
				microToMilliseconds(replicaClient.WriteLag.Microseconds),
				entity,
				*serverID,
				replicaClient.MeasuredAt,
			)
		}
		if isPgtypePresent(replicaClient.FlushLag.Status) {
			m.metricsChannel <- NewMetric(
				"replication.standby.lag.flush.ms",
				microToMilliseconds(replicaClient.FlushLag.Microseconds),
				entity,
				*serverID,
				replicaClient.MeasuredAt,
			)
		}
		if isPgtypePresent(replicaClient.ReplayLag.Status) {
			m.metricsChannel <- NewMetric(
				"replication.standby.lag.replay.ms",
				microToMilliseconds(replicaClient.ReplayLag.Microseconds),
				entity,
				*serverID,
				replicaClient.MeasuredAt,
			)
		}

		// bytes lag
		m.metricsChannel <- NewMetric(
			"replication.standby.lag.write.bytes",
			replicaClient.WriteLagBytes,
			entity,
			*serverID,
			replicaClient.MeasuredAt,
		)
		m.metricsChannel <- NewMetric(
			"replication.standby.lag.flush.bytes",
			replicaClient.FlushLagBytes,
			entity,
			*serverID,
			replicaClient.MeasuredAt,
		)
		m.metricsChannel <- NewMetric(
			"replication.standby.lag.replay.bytes",
			replicaClient.ReplayLagBytes,
			entity,
			*serverID,
			replicaClient.MeasuredAt,
		)

	}
}

//
// queries
//

func (m *ReplicationMonitor) FindReplicas(postgresClient *PostgresClient) []*ReplicaClient {
	var replicaClients []*ReplicaClient

	// write, flush and replay lag are only useful for sync replication which is what heroku uses
	query := `select application_name, client_addr, client_hostname, client_port,
						backend_start, backend_xmin, state, write_lag, flush_lag, replay_lag,
						pg_wal_lsn_diff(sent_lsn, write_lsn) as write_lag_bytes,
						pg_wal_lsn_diff(write_lsn, flush_lsn) as flush_lag_bytes,
						pg_wal_lsn_diff(flush_lsn, replay_lsn) as replay_lag_bytes,
						sync_priority, sync_state from pg_stat_replication` + postgresMonitorQueryComment()

	rows, err := postgresClient.client.Query(query)

	if err != nil {
		return []*ReplicaClient{}
	}
	defer rows.Close()

	for rows.Next() {
		var replicaClient ReplicaClient

		var applicationName sql.NullString

		err := rows.Scan(
			&applicationName,
			&replicaClient.ClientAddr,
			&replicaClient.ClientHostname,
			&replicaClient.ClientPort,
			&replicaClient.BackendStart,
			&replicaClient.Backendxmin,
			&replicaClient.State,
			&replicaClient.WriteLag,
			&replicaClient.FlushLag,
			&replicaClient.ReplayLag,
			&replicaClient.WriteLagBytes,
			&replicaClient.FlushLagBytes,
			&replicaClient.ReplayLagBytes,
			&replicaClient.SyncPriority,
			&replicaClient.SyncState,
		)
		if err != nil {
			continue
		}

		// set application name to application_name:backend_start to uniquely identify the standby
		if applicationName.Valid {
			if replicaClient.BackendStart.Valid {
				replicaClient.ApplicationName = fmt.Sprintf("%s:%d", applicationName.String, replicaClient.BackendStart.Time.Unix())
			} else {
				replicaClient.ApplicationName = applicationName.String
			}
		} else {
			replicaClient.ApplicationName = ""
		}

		replicaClient.MeasuredAt = time.Now().UTC().Unix()

		replicaClients = append(replicaClients, &replicaClient)
	}

	return replicaClients
}

func (m *ReplicationMonitor) FindReplica(postgresClient *PostgresClient) *Replica {
	var replica Replica
	var connInfo string

	// NOTE: PG 10 doesn't have sender_host available so we extract it from the conninfo
	// PG 11 adds this field https://www.postgresql.org/docs/11/monitoring-stats.html#PG-STAT-WAL-RECEIVER-VIEW
	query := `select status, conninfo from pg_stat_wal_receiver` + postgresMonitorQueryComment()

	// or use pg_is_in_recovery() to determine if it's a replica

	err := postgresClient.client.QueryRow(query).Scan(&replica.Status, &connInfo)

	if err != nil || connInfo == "" {
		return nil // return nil to not send replica with 0 lag
	}

	measuredAt := time.Now().UTC().Unix()
	replica.MeasuredAt = measuredAt
	lag := m.FindReplicationLag(postgresClient)
	if lag != nil {
		replica.Lag = *lag
	}

	primaryHost, applicationName := m.FindHostAndApplicationNameForPrimaryFromConnInfo(connInfo)

	replica.PrimaryHost = primaryHost
	replica.ApplicationName = applicationName

	// match server host with client config name
	for _, postgresClient := range m.postgresClients {
		if postgresClient.host == replica.PrimaryHost {
			replica.PrimaryConfigName = postgresClient.serverID.ConfigName
			break
		}
	}

	// get backend_start from pg_stat_activity for walreceiver process
	walReceiverBackendStart := m.FindWalReceiverBackendStart(postgresClient)

	// use backend_start with application name for unique application id
	if walReceiverBackendStart.Valid {
		replica.ApplicationName = fmt.Sprintf("%s:%d", replica.ApplicationName, walReceiverBackendStart.Time.Unix())
	}

	return &replica
}

func (m *ReplicationMonitor) FindWalReceiverBackendStart(postgresClient *PostgresClient) sql.NullTime {
	query := "select backend_start from pg_stat_activity where backend_type = 'walreceiver'" + postgresMonitorQueryComment()

	var backendStart sql.NullTime

	err := postgresClient.client.QueryRow(query).Scan(&backendStart)

	if err != nil {
		return backendStart
	}

	return backendStart
}

// this lag is only useful on active dbs since inactive dbs will show an ever increasing lag
// because the primary is not writing/updating
func (m *ReplicationMonitor) FindReplicationLag(postgresClient *PostgresClient) *pgtype.Interval {
	var lag pgtype.Interval
	// "select extract(epoch from coalesce(now() - pg_last_xact_replay_timestamp(), 0 * INTERVAL '1 minute'))::int AS lag" - as seconds?
	err := postgresClient.client.QueryRow("select now() - pg_last_xact_replay_timestamp() as lag" + postgresMonitorQueryComment()).Scan(&lag)
	if err != nil {
		return nil
	}
	return &lag
}

func (m *ReplicationMonitor) FindHostAndApplicationNameForPrimaryFromConnInfo(connInfo string) (string, string) {
	// example connInfo => user=postgres passfile=/etc/postgresql/recovery_pgpass channel_binding=prefer dbname=replication host=ec2-123-456-789.compute-1.amazonaws.com port=5432 application_name=follower fallback_application_name=walreceiver sslmode=prefer sslcompression=0 sslsni=1 ssl_min_protocol_version=TLSv1.2 gssencmode=prefer krbsrvname=postgres target_session_attrs=any

	host := ""
	applicationName := ""

	if connInfo == "" {
		return host, applicationName
	}

	for _, info := range strings.Split(connInfo, " ") {
		values := strings.Split(info, "=")
		if len(values) > 1 {
			key := values[0]
			value := values[1]

			if key == "host" {
				host = value
			} else if key == "application_name" {
				applicationName = value
			}
		}
	}

	return host, applicationName
}

func isPgtypePresent(s pgtype.Status) bool {
	return s == pgtype.Present // not null
}

func microToMilliseconds(microseconds int64) float64 {
	return float64(microseconds / 1000)
}
