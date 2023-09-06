package data

import (
	"agent/db"
	"agent/errors"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
)

func TestAddMetrics(t *testing.T) {
	data := &Data{}
	data.AddLogMetrics(map[string]string{
		"timestamp": "1648166371",
		"foo":       "12345",
	})

	metrics := data.LogMetrics

	assert.Equal(t, 1, len(metrics))

	metricEntries := metrics[0]
	assert.Equal(t, "1648166371", metricEntries["timestamp"])
	assert.Equal(t, "12345", metricEntries["foo"])
}

func TestAddPostgresServerNew(t *testing.T) {
	data := &Data{}

	postgresServer := &db.PostgresServer{
		ServerID: &db.ServerID{
			Name:          "GREEN",
			ConfigVarName: "GREEN_URL",
		},
	}

	data.AddPostgresServer(postgresServer)

	servers := data.PostgresServers
	assert.Equal(t, 1, len(servers))

	server := servers[0]
	assert.Equal(t, "GREEN_URL", server.ServerID.ConfigVarName)
	assert.Equal(t, "GREEN", server.ServerID.Name)
}

func TestAddPostgresServerExisting(t *testing.T) {
	data := &Data{}

	postgresServer := &db.PostgresServer{
		ServerID: &db.ServerID{
			Name:          "GREEN",
			ConfigVarName: "GREEN_URL",
		},
		MonitoredAt: int64(123456789),
	}

	data.AddPostgresServer(postgresServer)

	postgresServer2 := &db.PostgresServer{
		ServerID: &db.ServerID{
			Name:          "GREEN",
			ConfigVarName: "GREEN_URL",
		},
		MonitoredAt: int64(1234567891),
	}

	data.AddPostgresServer(postgresServer2)

	servers := data.PostgresServers
	assert.Equal(t, 1, len(servers))

	server := servers[0]
	assert.Equal(t, "GREEN_URL", server.ServerID.ConfigVarName)
	assert.Equal(t, "GREEN", server.ServerID.Name)
	assert.Equal(t, int64(1234567891), server.MonitoredAt)
}

func TestAddDatabase(t *testing.T) {
	data := &Data{}
	data.AddPostgresServer(&db.PostgresServer{
		ServerID: &db.ServerID{
			Name:          "GREEN",
			ConfigVarName: "GREEN_URL",
		},
		Version:     "10.19",
		MonitoredAt: 1649299369,
	})
	data.AddDatabase(&db.Database{
		ServerID: &db.ServerID{
			Name:          "GREEN",
			ConfigVarName: "GREEN_URL",
			Database:      "testDb",
		},
		Name: "testDb",
		Schemas: []*db.Schema{{
			Name: "public",
			Tables: []*db.Table{
				{
					Name:            "test",
					Schema:          "public",
					LiveRowEstimate: 3001,
					TotalBytes:      10234,
					IndexBytes:      5000,
					ToastBytes:      1000,
					TableBytes:      4234,
					BloatBytes:      50,
					BloatFactor:     0.2,
				},
			},
		}},
	})

	assert.Equal(t, 1, len(data.Databases))
	assert.Equal(t, "GREEN", data.Databases[0].ServerID.Name)
	assert.Equal(t, "testDb", data.Databases[0].Name)
	assert.Equal(t, 1, len(data.Databases[0].Schemas))
	assert.Equal(t, "public", data.Databases[0].Schemas[0].Name)
	assert.Equal(t, 1, len(data.Databases[0].Schemas[0].Tables))
	assert.Equal(t, "test", data.Databases[0].Schemas[0].Tables[0].Name)

	// add same db again with different schema
	data.AddDatabase(&db.Database{
		ServerID: &db.ServerID{
			Name:          "GREEN",
			ConfigVarName: "GREEN_URL",
			Database:      "testDb",
		},
		Name: "testDb",
		Schemas: []*db.Schema{{
			Name: "public",
			Tables: []*db.Table{
				{
					Name:            "test2",
					Schema:          "public",
					LiveRowEstimate: 3001,
					TotalBytes:      10234,
					IndexBytes:      5000,
					ToastBytes:      1000,
					TableBytes:      4234,
					BloatBytes:      50,
					BloatFactor:     0.2,
				},
			},
		}},
	})

	assert.Equal(t, 1, len(data.Databases))
	assert.Equal(t, "GREEN", data.Databases[0].ServerID.Name)
	assert.Equal(t, "testDb", data.Databases[0].Name)
	assert.Equal(t, 1, len(data.Databases[0].Schemas))
	assert.Equal(t, "public", data.Databases[0].Schemas[0].Name)
	assert.Equal(t, 1, len(data.Databases[0].Schemas[0].Tables))
	assert.Equal(t, "test2", data.Databases[0].Schemas[0].Tables[0].Name)
}

func TestAddReplication(t *testing.T) {
	data := &Data{}
	data.AddPostgresServer(&db.PostgresServer{
		ServerID: &db.ServerID{
			Name:          "GREEN",
			ConfigVarName: "GREEN_URL",
		},
		Version:     "10.19",
		MonitoredAt: 1649299369,
	})

	data.AddReplication(&db.Replication{
		ServerID: &db.ServerID{
			Name:          "GREEN",
			ConfigVarName: "GREEN_URL",
			Database:      "testDb",
		},
		Replica: &db.Replica{
			ApplicationName:   "follower",
			PrimaryHost:       "ec2-123-456-789.compute-1.amazonaws.com",
			PrimaryConfigName: "GREEN",
			Status:            "sync",
			Lag:               buildPgTypeInterval(12350),
			MeasuredAt:        123456789,
		},
		Replicas: []*db.ReplicaClient{
			{
				ApplicationName: "",
				ClientAddr:      sql.NullString{String: "1.2.3.4", Valid: true},
				ClientHostname:  sql.NullString{String: "", Valid: true},
				ClientPort:      sql.NullInt32{Int32: 12345, Valid: true},
				BackendStart:    sql.NullTime{Time: time.Date(2022, 11, 17, 20, 34, 58, 651387237, time.UTC), Valid: true},
				Backendxmin:     sql.NullInt64{Int64: 123456789, Valid: true},
				State:           sql.NullString{String: "streaming", Valid: true},
				WriteLag:        buildPgTypeInterval(2350),
				FlushLag:        buildPgTypeInterval(9350),
				ReplayLag:       buildPgTypeInterval(12350),
				SyncPriority:    sql.NullInt32{Int32: 1, Valid: true},
				SyncState:       sql.NullString{String: "sync", Valid: true},
				MeasuredAt:      123456789,
			},
		},
	})

	assert.Equal(t, 1, len(data.Replications))
	assert.Equal(t, "follower", data.Replications[0].Replica.ApplicationName)
	assert.Equal(t, 1, len(data.Replications[0].Replicas))
	assert.Equal(t, "1.2.3.4", data.Replications[0].Replicas[0].ClientAddr.String)

	data.AddReplication(&db.Replication{
		ServerID: &db.ServerID{
			Name:          "GREEN",
			ConfigVarName: "GREEN_URL",
			Database:      "testDb",
		},
		Replica: &db.Replica{
			ApplicationName:   "follower",
			PrimaryHost:       "ec2-123-456-789.compute-1.amazonaws.com",
			PrimaryConfigName: "GREEN",
			Status:            "sync",
			Lag:               buildPgTypeInterval(12350),
			MeasuredAt:        123456789,
		},
		Replicas: []*db.ReplicaClient{
			{
				ApplicationName: "",
				// replaces client addr
				ClientAddr:     sql.NullString{String: "1.2.3.5", Valid: true},
				ClientHostname: sql.NullString{String: "", Valid: true},
				ClientPort:     sql.NullInt32{Int32: 12345, Valid: true},
				BackendStart:   sql.NullTime{Time: time.Date(2022, 11, 17, 20, 34, 58, 651387237, time.UTC), Valid: true},
				Backendxmin:    sql.NullInt64{Int64: 123456789, Valid: true},
				State:          sql.NullString{String: "streaming", Valid: true},
				WriteLag:       buildPgTypeInterval(2350),
				FlushLag:       buildPgTypeInterval(9350),
				ReplayLag:      buildPgTypeInterval(12350),
				SyncPriority:   sql.NullInt32{Int32: 1, Valid: true},
				SyncState:      sql.NullString{String: "sync", Valid: true},
				MeasuredAt:     123456789,
			},
		},
	})

	assert.Equal(t, 1, len(data.Replications))
	assert.Equal(t, "follower", data.Replications[0].Replica.ApplicationName)
	assert.Equal(t, 1, len(data.Replications[0].Replicas))
	assert.Equal(t, "1.2.3.5", data.Replications[0].Replicas[0].ClientAddr.String)
}

func TestAddErrorReport_AddOne(t *testing.T) {
	data := &Data{}

	er := &errors.ErrorReport{}

	data.AddErrorReport(er)

	assert.Equal(t, 1, len(data.Errors))
}

func TestAddErrorReport_AddEleven(t *testing.T) {
	data := &Data{}

	n := 0
	for n < 15 {
		data.AddErrorReport(&errors.ErrorReport{})
		n += 1
	}

	assert.Equal(t, 10, len(data.Errors))
}

func TestAddErrorReport_Panic(t *testing.T) {
	data := &Data{}

	n := 0
	for n < 11 {
		data.AddErrorReport(&errors.ErrorReport{})
		n += 1
	}

	assert.Equal(t, 10, len(data.Errors))

	panic := &errors.ErrorReport{
		Panic: true,
	}
	data.AddErrorReport(panic)

	assert.Equal(t, 11, len(data.Errors))
	assert.True(t, data.Errors[10].Panic)
}

func TestCopyAndReset(t *testing.T) {
	data := &Data{}
	serverId := &db.ServerID{
		Name:          "GREEN",
		ConfigVarName: "GREEN_URL",
		Database:      "testDb",
	}
	data.AddLogMetrics(map[string]string{
		"timestamp": "1648166371",
		"foo":       "12345",
	})
	data.AddPostgresServer(&db.PostgresServer{
		ServerID: serverId,
	})
	data.AddDatabase(&db.Database{
		Name: "testDb",
	})
	data.AddReplication(&db.Replication{
		ServerID: serverId,
		Replica: &db.Replica{
			ApplicationName:   "follower",
			PrimaryHost:       "ec2-123-456-789.compute-1.amazonaws.com",
			PrimaryConfigName: "GREEN",
			Status:            "sync",
			Lag:               buildPgTypeInterval(12350),
			MeasuredAt:        123456789,
		},
		Replicas: []*db.ReplicaClient{
			{
				ApplicationName: "",
				ClientAddr:      sql.NullString{String: "1.2.3.4", Valid: true},
				ClientHostname:  sql.NullString{String: "", Valid: true},
				ClientPort:      sql.NullInt32{Int32: 12345, Valid: true},
				BackendStart:    sql.NullTime{Time: time.Date(2022, 11, 17, 20, 34, 58, 651387237, time.UTC), Valid: true},
				Backendxmin:     sql.NullInt64{Int64: 123456789, Valid: true},
				State:           sql.NullString{String: "streaming", Valid: true},
				WriteLag:        buildPgTypeInterval(2350),
				FlushLag:        buildPgTypeInterval(9350),
				ReplayLag:       buildPgTypeInterval(12350),
				SyncPriority:    sql.NullInt32{Int32: 1, Valid: true},
				SyncState:       sql.NullString{String: "sync", Valid: true},
				MeasuredAt:      123456789,
			},
		},
	})
	data.AddMetrics([]*db.Metric{
		{
			Name:       "metric",
			Value:      10,
			Entity:     "table/foo",
			ServerID:   *serverId,
			MeasuredAt: 123456789,
		},
	})
	data.AddSetting(&db.Setting{
		Name:  "foo_setting",
		Value: "10",
	})
	data.AddErrorReport(&errors.ErrorReport{})

	copiedData := data.CopyAndReset()

	expectedEmptyData := &Data{
		LogMetrics:      []LogMetrics{},
		PostgresServers: []db.PostgresServer{},
		Databases:       []db.Database{},
		Replications:    []db.Replication{},
		Metrics:         []db.Metric{},
		Settings:        []db.Setting{},
		QueryStats:      []db.QueryStats{},
		Errors:          []errors.ErrorReport{},
	}
	assert.Equal(t, expectedEmptyData, data)

	metricCollection := LogMetrics{
		"timestamp": "1648166371",
		"foo":       "12345",
	}

	assert.Equal(t, 1, len(copiedData.LogMetrics))

	eq := reflect.DeepEqual(metricCollection, copiedData.LogMetrics[0])
	assert.True(t, eq)

	assert.Equal(t, 1, len(copiedData.Metrics))
	metric := copiedData.Metrics[0]
	assert.Equal(t, "metric", metric.Name)
	assert.Equal(t, 10.0, metric.Value)
	assert.Equal(t, "GREEN", metric.ServerID.Name)

	servers := copiedData.PostgresServers
	assert.Equal(t, 1, len(servers))

	server := servers[0]
	assert.Equal(t, "GREEN_URL", server.ServerID.ConfigVarName)
	assert.Equal(t, "GREEN", server.ServerID.Name)

	assert.Equal(t, 1, len(copiedData.Replications))
	assert.Equal(t, "follower", copiedData.Replications[0].Replica.ApplicationName)
	assert.Equal(t, "1.2.3.4", copiedData.Replications[0].Replicas[0].ClientAddr.String)

	assert.Equal(t, 1, len(copiedData.Errors))
}

func buildPgTypeInterval(microseconds int64) pgtype.Interval {
	return pgtype.Interval{Days: 0, Months: 0, Microseconds: microseconds, Status: pgtype.Present}
}
