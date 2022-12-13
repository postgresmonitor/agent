package api

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"io"
	"postgres-monitor/config"
	"postgres-monitor/data"
	"postgres-monitor/db"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
)

func TestNewReportRequest(t *testing.T) {
	config := config.Config{
		Version: "1.0.0",
	}

	data := &data.Data{}
	serverId := &db.ServerID{
		ConfigName:    "GREEN",
		ConfigVarName: "GREEN_URL",
		Database:      "testDb",
	}

	data.AddLogMetrics(map[string]string{
		"addon":          "postgresql-contoured-12345",
		"avg_query":      "8751",
		"avg_recv":       "37568",
		"avg_sent":       "1339005",
		"client_active":  "56",
		"client_waiting": "0",
		"max_wait":       "0",
		"server_active":  "0",
		"server_idle":    "16",
		"source":         "pgbouncer",
		"timestamp":      "1648166379",
	})
	data.AddPostgresServer(&db.PostgresServer{
		ServerID:    serverId,
		Version:     "10.19 (Ubuntu 10.19-2.pgdg20.04+1)",
		MonitoredAt: 1649299369,
	})
	data.AddDatabase(&db.Database{
		ServerID: serverId,
		Name:     "testDb",
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
	data.AddReplication(&db.Replication{
		ServerID: serverId,
		Replica: &db.Replica{
			ApplicationName:   "follower:651387237",
			PrimaryHost:       "ec2-123-456-789.compute-1.amazonaws.com",
			PrimaryConfigName: "GREEN",
			Status:            "sync",
			Lag:               buildPgTypeInterval(12350),
			MeasuredAt:        123456789,
		},
		Replicas: []*db.ReplicaClient{
			{
				ApplicationName: "follower:651387237",
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
	data.AddMetric(&db.Metric{
		Name:       "connections.used",
		Value:      33,
		Entity:     "",
		ServerID:   *serverId,
		MeasuredAt: 123456789,
	})
	data.AddMetric(&db.Metric{
		Name:       "connections.max",
		Value:      500,
		Entity:     "",
		ServerID:   *serverId,
		MeasuredAt: 123456789,
	})
	data.AddMetric(&db.Metric{
		Name:       "connections.used",
		Value:      35,
		Entity:     "",
		ServerID:   *serverId,
		MeasuredAt: 123456799,
	})
	data.AddMetric(&db.Metric{
		Name:       "connections.max",
		Value:      500,
		Entity:     "",
		ServerID:   *serverId,
		MeasuredAt: 123456799,
	})
	data.AddQueryStats(&db.QueryStats{
		ServerID:            serverId,
		QueryId:             1855550508,
		Query:               "UPDATE \"stats\" SET \"value\" = $1, \"updated_at\" = $2 WHERE \"stats\".\"id\" = $3",
		Comment:             "/*app:worker-1*/",
		Explain:             "explain-foo",
		Calls:               10,
		TotalTime:           3.5,
		MinTime:             0.01231,
		MaxTime:             4.895439,
		MeanTime:            0.35,
		Rows:                10,
		SharedBlocksHit:     3,
		SharedBlocksRead:    0,
		SharedBlocksDirtied: 1,
		SharedBlocksWritten: 0,
		LocalBlocksHit:      0,
		LocalBlocksRead:     0,
		LocalBlocksDirtied:  0,
		LocalBlocksWritten:  0,
		TempBlocksRead:      0,
		TempBlocksWritten:   0,
		BlockReadTime:       3.948159,
		BlockWriteTime:      10,
	})

	request := NewReportRequest(config, data, 1649303496)

	assert.Equal(t, "1.0.0", request.Client.Version)

	json, _ := json.Marshal(request)

	assert.Equal(t, "{\"log_metrics\":[{\"addon\":\"postgresql-contoured-12345\",\"avg_query\":\"8751\",\"avg_recv\":\"37568\",\"avg_sent\":\"1339005\",\"client_active\":\"56\",\"client_waiting\":\"0\",\"max_wait\":\"0\",\"server_active\":\"0\",\"server_idle\":\"16\",\"source\":\"pgbouncer\",\"timestamp\":\"1648166379\"}],\"servers\":[{\"config_name\":\"GREEN\",\"config_var_name\":\"GREEN_URL\",\"databases\":[{\"name\":\"testDb\",\"schemas\":[{\"name\":\"public\",\"tables\":[{\"name\":\"test\",\"total_bytes\":10234,\"index_bytes\":5000,\"toast_bytes\":1000,\"table_bytes\":4234,\"bloat_bytes\":50,\"bloat_factor\":0.2,\"live_row_estimate\":3001}]}]}],\"replica\":{\"application_name\":\"follower:651387237\",\"primary_config_name\":\"GREEN\",\"status\":\"sync\"},\"replicas\":[{\"application_name\":\"follower:651387237\",\"backend_start\":1668717298,\"backend_xmin\":123456789,\"state\":\"streaming\",\"sync_priority\":1,\"sync_state\":\"sync\"}],\"metrics\":[{\"name\":\"connections.used\",\"values\":[{\"value\":33,\"measured_at\":123456789},{\"value\":35,\"measured_at\":123456799}]},{\"name\":\"connections.max\",\"values\":[{\"value\":500,\"measured_at\":123456789},{\"value\":500,\"measured_at\":123456799}]}],\"queries\":{\"stats\":[{\"database\":\"testDb\",\"query_id\":1855550508,\"query\":\"UPDATE \\\"stats\\\" SET \\\"value\\\" = $1, \\\"updated_at\\\" = $2 WHERE \\\"stats\\\".\\\"id\\\" = $3\",\"comment\":\"/*app:worker-1*/\",\"explain\":\"explain-foo\",\"calls\":10,\"time\":3.5,\"mean_time\":0.35,\"min_time\":0.02,\"max_time\":4.9,\"rows\":10,\"shared_blocks_hit\":3,\"shared_blocks_dirtied\":1,\"block_read_time\":3.95,\"block_write_time\":10}]},\"version\":\"10.19 (Ubuntu 10.19-2.pgdg20.04+1)\",\"monitored_at\":1649299369}],\"reported_at\":1649303496,\"client\":{\"uuid\":\"00000000-0000-0000-0000-000000000000\",\"version\":\"1.0.0\"}}", string(json))
}

func TestNewReportRequestReplicas(t *testing.T) {
	config := config.Config{
		Version: "1.0.0",
	}

	data := &data.Data{}
	data.AddPostgresServer(&db.PostgresServer{
		ServerID: &db.ServerID{
			ConfigName:    "GREEN",
			ConfigVarName: "GREEN_URL",
		},
		Version:     "10.19 (Ubuntu 10.19-2.pgdg20.04+1)",
		MonitoredAt: 1649299369,
	})
	data.AddReplication(&db.Replication{
		ServerID: &db.ServerID{
			ConfigName:    "GREEN",
			ConfigVarName: "GREEN_URL",
			Database:      "testDb",
		},
		Replica: &db.Replica{
			ApplicationName:   "follower:651387237",
			PrimaryHost:       "ec2-123-456-789.compute-1.amazonaws.com",
			PrimaryConfigName: "GREEN",
			Status:            "sync",
			Lag:               buildPgTypeInterval(12350),
			MeasuredAt:        123456789,
		},
	})
	data.AddReplication(&db.Replication{
		ServerID: &db.ServerID{
			ConfigName:    "GREEN",
			ConfigVarName: "GREEN_URL",
			Database:      "testDb",
		},
		Replica: &db.Replica{
			ApplicationName:   "follower:651387237",
			PrimaryHost:       "ec2-123-456-789.compute-1.amazonaws.com",
			PrimaryConfigName: "GREEN",
			Status:            "sync",
			Lag:               buildPgTypeInterval(15350),
			MeasuredAt:        123456790,
		},
	})

	request := NewReportRequest(config, data, 1649303496)

	assert.Equal(t, "1.0.0", request.Client.Version)

	json, _ := json.Marshal(request)

	assert.Equal(t, "{\"servers\":[{\"config_name\":\"GREEN\",\"config_var_name\":\"GREEN_URL\",\"replica\":{\"application_name\":\"follower:651387237\",\"primary_config_name\":\"GREEN\",\"status\":\"sync\"},\"version\":\"10.19 (Ubuntu 10.19-2.pgdg20.04+1)\",\"monitored_at\":1649299369}],\"reported_at\":1649303496,\"client\":{\"uuid\":\"00000000-0000-0000-0000-000000000000\",\"version\":\"1.0.0\"}}", string(json))
}

func TestValid(t *testing.T) {
	config := config.Config{
		Version: "1.0.0",
	}

	data := &data.Data{}
	request := NewReportRequest(config, data, 1649303496)
	assert.False(t, request.Valid())

	data.AddLogMetrics(map[string]string{
		"addon":          "postgresql-contoured-12345",
		"avg_query":      "8751",
		"avg_recv":       "37568",
		"avg_sent":       "1339005",
		"client_active":  "56",
		"client_waiting": "0",
		"max_wait":       "0",
		"server_active":  "0",
		"server_idle":    "16",
		"source":         "pgbouncer",
		"timestamp":      "1648166379",
	})
	request = NewReportRequest(config, data, 1649303496)
	assert.True(t, request.Valid())
}

func TestToJSON(t *testing.T) {
	config := config.Config{
		Version: "1.0.0",
	}

	data := &data.Data{}

	data.AddLogMetrics(map[string]string{
		"addon":          "postgresql-contoured-12345",
		"avg_query":      "8751",
		"avg_recv":       "37568",
		"avg_sent":       "1339005",
		"client_active":  "56",
		"client_waiting": "0",
		"max_wait":       "0",
		"server_active":  "0",
		"server_idle":    "16",
		"source":         "pgbouncer",
		"timestamp":      "1648166379",
	})

	request := NewReportRequest(config, data, 1649303496)
	json, _ := request.ToJSON()
	assert.Equal(t, "{\"log_metrics\":[{\"addon\":\"postgresql-contoured-12345\",\"avg_query\":\"8751\",\"avg_recv\":\"37568\",\"avg_sent\":\"1339005\",\"client_active\":\"56\",\"client_waiting\":\"0\",\"max_wait\":\"0\",\"server_active\":\"0\",\"server_idle\":\"16\",\"source\":\"pgbouncer\",\"timestamp\":\"1648166379\"}],\"reported_at\":1649303496,\"client\":{\"uuid\":\"00000000-0000-0000-0000-000000000000\",\"version\":\"1.0.0\"}}", string(json))
}

func TestToCompressedJSON(t *testing.T) {
	config := config.Config{
		Version: "1.0.0",
	}

	data := &data.Data{}

	data.AddLogMetrics(map[string]string{
		"addon":          "postgresql-contoured-12345",
		"avg_query":      "8751",
		"avg_recv":       "37568",
		"avg_sent":       "1339005",
		"client_active":  "56",
		"client_waiting": "0",
		"max_wait":       "0",
		"server_active":  "0",
		"server_idle":    "16",
		"source":         "pgbouncer",
		"timestamp":      "1648166379",
	})

	request := NewReportRequest(config, data, 1649303496)
	compressed, _ := request.ToCompressedJSON()

	gz, _ := gzip.NewReader(compressed)
	defer gz.Close()

	var buffer bytes.Buffer
	_, _ = io.Copy(&buffer, gz)

	json := string(buffer.Bytes())
	assert.Equal(t, "{\"log_metrics\":[{\"addon\":\"postgresql-contoured-12345\",\"avg_query\":\"8751\",\"avg_recv\":\"37568\",\"avg_sent\":\"1339005\",\"client_active\":\"56\",\"client_waiting\":\"0\",\"max_wait\":\"0\",\"server_active\":\"0\",\"server_idle\":\"16\",\"source\":\"pgbouncer\",\"timestamp\":\"1648166379\"}],\"reported_at\":1649303496,\"client\":{\"uuid\":\"00000000-0000-0000-0000-000000000000\",\"version\":\"1.0.0\"}}", string(json))
}

func buildPgTypeInterval(microseconds int64) pgtype.Interval {
	return pgtype.Interval{Days: 0, Months: 0, Microseconds: microseconds, Status: pgtype.Present}
}
