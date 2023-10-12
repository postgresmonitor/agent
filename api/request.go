package api

import (
	"agent/aws"
	"agent/config"
	"agent/data"
	"agent/db"
	"agent/errors"
	"agent/util"
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
)

//
// These are api request structs that define the json message payload to the api
//

type ReportRequest struct {
	LogMetrics               []LogMetrics     `json:"log_metrics,omitempty"`
	PostgresServers          []PostgresServer `json:"servers,omitempty"`
	ReportedAt               int64            `json:"reported_at"`
	LogTestMessageReceivedAt int64            `json:"log_test_message_received_at,omitempty"`
	Agent                    Agent            `json:"agent"`
}

type LogMetrics map[string]string

type PostgresServer struct {
	// ex. GREEN
	ConfigName string `json:"config_name"`

	// ex. GREEN_URL
	ConfigVarName string `json:"config_var_name"`

	Platform string `json:"platform,omitempty"`

	Databases []*Database `json:"databases,omitempty"`

	Replica  *Replica         `json:"replica,omitempty"`
	Replicas []*ReplicaClient `json:"replicas,omitempty"`

	Metrics []*Metric `json:"metrics,omitempty"`
	Queries *Queries  `json:"queries,omitempty"`

	MaxConnections int64        `json:"max_connections,omitempty"`
	PgBouncer      *PgBouncer   `json:"pg_bouncer,omitempty"`
	RDSInstance    *RDSInstance `json:"rds_instance,omitempty"`
	Settings       []*Setting   `json:"settings,omitempty"`
	Version        string       `json:"version"`
	MonitoredAt    int64        `json:"monitored_at"`
}

type PgBouncer struct {
	MaxServerConnections int64  `json:"max_server_connections,omitempty"`
	Version              string `json:"version,omitempty"`
}

type RDSInstance struct {
	EnhancedMonitoringEnabled bool      `json:"enhanced_monitoring"`
	InstanceID                string    `json:"instance_id,omitempty"`
	InstanceClass             string    `json:"instance_class,omitempty"`
	Metrics                   []*Metric `json:"metrics,omitempty"`
}

type Metric struct {
	Name   string        `json:"name"`
	Entity string        `json:"entity,omitempty"`
	Values []MetricValue `json:"values,omitempty"`
}

type MetricValue struct {
	Value      float64 `json:"value"`
	MeasuredAt int64   `json:"measured_at"`
}

type Queries struct {
	Stats []*Query `json:"stats,omitempty"`
}

type Query struct {
	// use omitempty to not send up 0 values
	Database            string  `json:"database,omitempty"`
	QueryId             int64   `json:"query_id,omitempty"`
	Fingerprint         string  `json:"fingerprint,omitempty"`
	Query               string  `json:"query,omitempty"`
	Comment             string  `json:"comment,omitempty"`
	Explain             string  `json:"explain,omitempty"`
	Calls               int64   `json:"calls,omitempty"`
	TotalTime           float64 `json:"time,omitempty"`
	MeanTime            float64 `json:"mean_time,omitempty"`
	MinTime             float64 `json:"min_time,omitempty"`
	MaxTime             float64 `json:"max_time,omitempty"`
	Rows                int64   `json:"rows,omitempty"`
	SharedBlocksHit     int64   `json:"shared_blocks_hit,omitempty"`
	SharedBlocksRead    int64   `json:"shared_blocks_read,omitempty"`
	SharedBlocksDirtied int64   `json:"shared_blocks_dirtied,omitempty"`
	SharedBlocksWritten int64   `json:"shared_blocks_written,omitempty"`
	LocalBlocksHit      int64   `json:"local_blocks_hit,omitempty"`
	LocalBlocksRead     int64   `json:"local_blocks_read,omitempty"`
	LocalBlocksDirtied  int64   `json:"local_blocks_dirtied,omitempty"`
	LocalBlocksWritten  int64   `json:"local_blocks_written,omitempty"`
	TempBlocksRead      int64   `json:"temp_blocks_read,omitempty"`
	TempBlocksWritten   int64   `json:"temp_blocks_written,omitempty"`
	BlockReadTime       float64 `json:"block_read_time,omitempty"`
	BlockWriteTime      float64 `json:"block_write_time,omitempty"`
	BlockTotalTime      float64 `json:"block_total_time,omitempty"`
	MeasuredAt          int64   `json:"measured_at,omitempty"`
}

type Database struct {
	Name    string    `json:"name"`
	Schemas []*Schema `json:"schemas"`
}

type Schema struct {
	Name   string   `json:"name"`
	Tables []*Table `json:"tables,omitempty"`
}

type Table struct {
	Name                     string  `json:"name"`
	TotalBytes               int64   `json:"total_bytes,omitempty"`
	TotalBytesTotal          int64   `json:"total_bytes_total,omitempty"`
	IndexBytes               int64   `json:"index_bytes,omitempty"`
	IndexBytesTotal          int64   `json:"index_bytes_total,omitempty"`
	ToastBytes               int64   `json:"toast_bytes,omitempty"`
	ToastBytesTotal          int64   `json:"toast_bytes_total,omitempty"`
	TableBytes               int64   `json:"table_bytes,omitempty"`
	TableBytesTotal          int64   `json:"table_bytes_total,omitempty"`
	BloatBytes               int64   `json:"bloat_bytes,omitempty"`
	BloatBytesTotal          int64   `json:"bloat_bytes_total,omitempty"`
	BloatFactor              float64 `json:"bloat_factor,omitempty"`
	SequentialScans          int64   `json:"sequential_scans,omitempty"`
	SequentialScanReadRows   int64   `json:"sequential_scan_read_rows,omitempty"`
	IndexScans               int64   `json:"index_scans,omitempty"`
	IndexScanReadRows        int64   `json:"index_scan_read_rows,omitempty"`
	InsertedRows             int64   `json:"inserted_rows,omitempty"`
	UpdatedRows              int64   `json:"updated_rows,omitempty"`
	DeletedRows              int64   `json:"deleted_rows,omitempty"`
	LiveRowEstimate          int64   `json:"live_row_estimate,omitempty"`
	LiveRowEstimateTotal     int64   `json:"live_row_estimate_total,omitempty"`
	DeadRowEstimate          int64   `json:"dead_row_estimate,omitempty"`
	DeadRowEstimateTotal     int64   `json:"dead_row_estimate_total,omitempty"`
	ModifiedRowsSinceAnalyze int64   `json:"modified_rows_since_analyze,omitempty"`
	LastVacuumAt             int64   `json:"last_vacuum_at,omitempty"`
	LastAutovacuumAt         int64   `json:"last_autovacuum_at,omitempty"`
	LastAnalyzeAt            int64   `json:"last_analyze_at,omitempty"`
	LastAutoanalyzeAt        int64   `json:"last_autoanalyze_at,omitempty"`
	VacuumCount              int64   `json:"vacuum_count,omitempty"`
	AutovacuumCount          int64   `json:"autovacuum_count,omitempty"`
	AnalyzeCount             int64   `json:"analyze_count,omitempty"`
	AutoanalyzeCount         int64   `json:"autoanalyze_count,omitempty"`
	DiskBlocksRead           int64   `json:"blocks_read,omitempty"`
	DiskBlocksHit            int64   `json:"blocks_hit,omitempty"`
	DiskBlocksHitPercent     float64 `json:"blocks_hit_percent,omitempty"`
	DiskIndexBlocksRead      int64   `json:"index_blocks_read,omitempty"`
	DiskIndexBlocksHit       int64   `json:"index_blocks_hit,omitempty"`
	DiskToastBlocksRead      int64   `json:"toast_blocks_read,omitempty"`
	DiskToastBlocksHit       int64   `json:"toast_blocks_hit,omitempty"`
	DiskToastIndexBlocksRead int64   `json:"toast_index_blocks_read,omitempty"`
	DiskToastIndexBlocksHit  int64   `json:"toast_index_blocks_hit,omitempty"`

	Columns []*Column `json:"columns,omitempty"`
	Indexes []*Index  `json:"indexes,omitempty"`
}

type Column struct {
	Name             string `json:"name,omitempty"`
	Default          string `json:"default,omitempty"`
	Type             string `json:"type,omitempty"`
	Nullable         bool   `json:"nullable,omitempty"`
	MaxLength        int    `json:"max_length,omitempty"`
	NumericPrecision int    `json:"precision,omitempty"`
	NumericScale     int    `json:"scale,omitempty"`
	IntervalType     string `json:"interval_type,omitempty"`
	IsIdentity       bool   `json:"is_identity,omitempty"`
}

type Index struct {
	Name            string  `json:"name"`
	Unique          bool    `json:"unique"`
	Unused          bool    `json:"unused"`
	Valid           bool    `json:"valid"`
	Bytes           int64   `json:"bytes,omitempty"`
	BytesTotal      int64   `json:"bytes_total,omitempty"`
	BloatBytes      int64   `json:"bloat_bytes,omitempty"`
	BloatBytesTotal int64   `json:"bloat_bytes_total,omitempty"`
	BloatFactor     float64 `json:"bloat_factor,omitempty"`
	Scans           int64   `json:"scans,omitempty"`
	DiskBlocksRead  int64   `json:"blocks_read,omitempty"`
	DiskBlocksHit   int64   `json:"blocks_hit,omitempty"`
	Definition      string  `json:"definition,omitempty"`
}

type Agent struct {
	UUID         string   `json:"uuid"`
	Version      string   `json:"version"`
	Stats        *Stats   `json:"stats,omitempty"`
	Errors       []*Error `json:"errors,omitempty"`
	HostPlatform string   `json:"host_platform,omitempty"`
}

type Stats struct {
	LogStats *LogStats `json:"logs,omitempty"`
}

type LogStats struct {
	Received            int `json:"received,omitempty"`
	Postgres            int `json:"postgres,omitempty"`
	Handled             int `json:"handled,omitempty"`
	MetricLines         int `json:"metric_lines,omitempty"`
	MetricsLinesDropped int `json:"metrics_dropped,omitempty"`
	SlowQueries         int `json:"slow_queries,omitempty"`
	SlowQueriesDropped  int `json:"slow_queries_dropped,omitempty"`
}

// NOTE: we do not want to expose replica server or client hostnames, IPs or ports
type Replica struct {
	ApplicationName   string `json:"application_name,omitempty"`
	PrimaryConfigName string `json:"primary_config_name,omitempty"` // ex. GREEN
	Status            string `json:"status,omitempty"`
}

// we track agent errors and panics to proactively be aware of agent issues
type Error struct {
	Error      string `json:"error,omitempty"`
	Panic      bool   `json:"panic,omitempty"`
	StackTrace string `json:"stack_trace,omitempty"`
}

type ReplicaClient struct {
	ApplicationName string `json:"application_name,omitempty"`
	BackendStart    int64  `json:"backend_start,omitempty"`
	Backendxmin     int64  `json:"backend_xmin,omitempty"`
	State           string `json:"state,omitempty"`
	SyncPriority    int    `json:"sync_priority"`
	SyncState       string `json:"sync_state,omitempty"`
}

type Setting struct {
	Name           string `json:"name,omitempty"`
	Value          string `json:"value,omitempty"`
	Unit           string `json:"unit,omitempty"`
	Category       string `json:"category,omitempty"`
	Description    string `json:"description,omitempty"`
	Context        string `json:"context,omitempty"`
	VarType        string `json:"var_type,omitempty"`
	Source         string `json:"source,omitempty"`
	MinVal         string `json:"min_val,omitempty"`
	MaxVal         string `json:"max_val,omitempty"`
	EnumVals       string `json:"enum_vals,omitempty"`
	BootVal        string `json:"boot_val,omitempty"`
	ResetVal       string `json:"reset_val,omitempty"`
	PendingRestart bool   `json:"pending_restart"`
}

func NewReportRequest(config config.Config, data *data.Data, reportedAt int64, stats *util.Stats) ReportRequest {
	return ReportRequest{
		LogMetrics:               ConvertLogMetrics(data.LogMetrics),
		PostgresServers:          ConvertPostgresServers(data.PostgresServers, data.Databases, data.Replications, data.Metrics, data.Settings, data.QueryStats, data.RDSMetrics),
		LogTestMessageReceivedAt: data.LogTestMessageReceivedAt,
		ReportedAt:               reportedAt,
		Agent: Agent{
			UUID:         config.UUID.String(),
			Version:      config.Version,
			Stats:        ConvertStats(stats),
			Errors:       ConvertErrors(data.Errors),
			HostPlatform: config.AgentHostPlatform,
		},
	}
}

// Don't send an empty request
func (r *ReportRequest) Valid() bool {
	return len(r.LogMetrics) != 0 || len(r.PostgresServers) != 0 || r.LogTestMessageReceivedAt != 0
}

// Returns JSON as bytes
func (r *ReportRequest) ToJSON() ([]byte, error) {
	json, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	return json, nil
}

// a quick test showed a 7.8x space saving when compared to raw json
func (r *ReportRequest) ToCompressedJSON() (*bytes.Buffer, error) {
	json, err := r.ToJSON()
	if err != nil {
		return nil, err
	}

	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	defer gz.Close()
	gz.Write(json)

	return &buffer, nil
}

func ConvertLogMetrics(from []data.LogMetrics) []LogMetrics {
	to := []LogMetrics{}
	for _, metric := range from {
		to = append(to, LogMetrics(metric))
	}
	return to
}

func ConvertPostgresServers(fromServers []db.PostgresServer, fromDbs []db.Database, fromReplications []db.Replication, fromMetrics []db.Metric, fromSettings []db.Setting, fromQueryStats []db.QueryStats, fromRDSInstanceMetrics []aws.RDSInstanceMetrics) []PostgresServer {
	to := []PostgresServer{}

	for _, fromServer := range fromServers {
		toServer := PostgresServer{
			ConfigVarName:  fromServer.ServerID.ConfigVarName,
			ConfigName:     fromServer.ServerID.Name,
			Platform:       fromServer.Platform,
			MaxConnections: fromServer.MaxConnections,
			Settings:       ConvertSettings(fromSettings, fromServer),
			Version:        fromServer.Version,
			MonitoredAt:    fromServer.MonitoredAt,
		}
		if fromServer.PgBouncer != nil {
			toServer.PgBouncer = &PgBouncer{
				MaxServerConnections: fromServer.PgBouncer.MaxServerConnections,
				Version:              fromServer.PgBouncer.Version,
			}
		}
		if len(fromRDSInstanceMetrics) > 0 {
			AddRDSInstance(&toServer, fromRDSInstanceMetrics)
		}

		// convert matching dbs
		for _, database := range fromDbs {
			// only one tracked database + schema per database so no aggregation needed
			if database.ServerID.Name == fromServer.ServerID.Name {
				toServer.Databases = append(toServer.Databases, ConvertDatabase(&database))
			}
		}

		// convert replica/replicas
		for _, fromReplication := range fromReplications {
			// only one tracked replica + replica clients per server so no aggregation needed
			if fromReplication.ServerID.Name == fromServer.ServerID.Name {
				toServer.Replica = ConvertReplica(fromReplication.Replica)
				toServer.Replicas = ConvertReplicas(fromReplication.Replicas)
			}
		}

		toServer.Metrics = ConvertMetrics(fromServer.ServerID.Name, fromMetrics)
		toServer.Queries = ConvertQueries(fromServer.ServerID.Name, fromQueryStats)

		to = append(to, toServer)
	}

	return to
}

func ConvertMetrics(name string, fromMetrics []db.Metric) []*Metric {
	var metrics []*Metric

	// unique metric name and entity id tuples
	var metricIDs [][]string

	// track unique metric names
	for _, from := range fromMetrics {
		// restrict to the current server
		if from.ServerID.Name == name {
			var found bool
			for _, metricID := range metricIDs {
				if metricID[0] == from.Name && metricID[1] == from.Entity {
					found = true
					break
				}
			}
			if !found {
				metricIDs = append(metricIDs, []string{from.Name, from.Entity})
			}
		}
	}

	// aggregate metrics by metric name and entity
	for _, metricID := range metricIDs {
		var metric *Metric
		var metricValues []MetricValue
		for _, from := range fromMetrics {
			// restrict to the current server plus metric name & entity
			if from.ServerID.Name == name && from.Name == metricID[0] && from.Entity == metricID[1] {
				if metric == nil {
					metric = &Metric{
						Name:   from.Name,
						Entity: from.Entity,
					}
				}

				metricValues = append(metricValues, MetricValue{Value: util.Round4(from.Value), MeasuredAt: from.MeasuredAt})
			}
		}

		metric.Values = metricValues
		metrics = append(metrics, metric)
	}

	return metrics
}

func AddRDSInstance(server *PostgresServer, fromRDSInstanceMetrics []aws.RDSInstanceMetrics) {
	var fromRDSInstance *aws.RDSInstanceMetrics

	for _, fromRDSInstanceMetric := range fromRDSInstanceMetrics {
		if fromRDSInstanceMetric.RDSInstance.InstanceID == server.ConfigName {
			if (fromRDSInstanceMetric.RDSInstance.IsAurora && server.Platform == db.AuroraPlatform) || (!fromRDSInstanceMetric.RDSInstance.IsAurora && server.Platform == db.RDSPlatform) {
				fromRDSInstance = &fromRDSInstanceMetric
				break
			}
		}
	}

	if fromRDSInstance != nil {
		server.RDSInstance = &RDSInstance{
			EnhancedMonitoringEnabled: fromRDSInstance.RDSInstance.EnhancedMonitoringEnabled,
			InstanceID:                fromRDSInstance.RDSInstance.InstanceID,
			InstanceClass:             fromRDSInstance.RDSInstance.InstanceClass,
			Metrics:                   ConvertRDSMetrics(fromRDSInstance.MetricResults),
		}
	}
}

func ConvertRDSMetrics(fromRDSMetrics []aws.MetricResult) []*Metric {
	var metrics []*Metric

	for _, rdsMetric := range fromRDSMetrics {
		metric := &Metric{
			Name: rdsMetric.MetricName,
		}
		var values []MetricValue
		for _, datapoint := range rdsMetric.Datapoints {
			values = append(values, MetricValue{Value: util.Round4(datapoint.Value), MeasuredAt: datapoint.Time.Unix()})
		}
		if len(values) > 0 {
			metric.Values = values
			metrics = append(metrics, metric)
		}
	}

	return metrics
}

func ConvertQueries(name string, fromQueryStats []db.QueryStats) *Queries {
	queries := &Queries{}
	var queryStats []*Query

	for _, fromStats := range fromQueryStats {
		if fromStats.ServerID.Name == name {
			queryStats = append(queryStats, ConvertQueryStats(fromStats))
		}
	}

	if len(queryStats) > 0 {
		queries.Stats = queryStats
		return queries
	} else {
		return nil
	}
}

func ConvertQueryStats(fromStats db.QueryStats) *Query {
	return &Query{
		Database:            fromStats.ServerID.Database,
		QueryId:             fromStats.QueryId,
		Fingerprint:         fromStats.Fingerprint,
		Query:               fromStats.Query,
		Comment:             fromStats.Comment,
		Explain:             fromStats.Explain,
		Calls:               fromStats.Calls,
		TotalTime:           util.Round(fromStats.TotalTime),
		MeanTime:            util.Round(fromStats.MeanTime),
		MinTime:             util.Round(fromStats.MinTime),
		MaxTime:             util.Round(fromStats.MaxTime),
		Rows:                fromStats.Rows,
		SharedBlocksHit:     fromStats.SharedBlocksHit,
		SharedBlocksRead:    fromStats.SharedBlocksRead,
		SharedBlocksDirtied: fromStats.SharedBlocksDirtied,
		SharedBlocksWritten: fromStats.SharedBlocksWritten,
		LocalBlocksHit:      fromStats.LocalBlocksHit,
		LocalBlocksRead:     fromStats.LocalBlocksRead,
		LocalBlocksDirtied:  fromStats.LocalBlocksDirtied,
		LocalBlocksWritten:  fromStats.LocalBlocksWritten,
		TempBlocksRead:      fromStats.TempBlocksRead,
		TempBlocksWritten:   fromStats.TempBlocksWritten,
		BlockReadTime:       util.Round(fromStats.BlockReadTime),
		BlockWriteTime:      util.Round(fromStats.BlockWriteTime),
		BlockTotalTime:      util.Round(fromStats.TotalBlockIOTime),
		MeasuredAt:          fromStats.MeasuredAt,
	}
}

func ConvertDatabases(from []*db.Database) []*Database {
	to := []*Database{}

	for _, fromDatabase := range from {
		toDatabase := &Database{
			Name:    fromDatabase.Name,
			Schemas: ConvertSchemas(fromDatabase.Schemas),
		}
		to = append(to, toDatabase)
	}

	return to
}

func ConvertDatabase(from *db.Database) *Database {
	return &Database{
		Name:    from.Name,
		Schemas: ConvertSchemas(from.Schemas),
	}
}

func ConvertSchemas(from []*db.Schema) []*Schema {
	to := []*Schema{}

	for _, fromSchema := range from {
		toSchema := &Schema{
			Name:   fromSchema.Name,
			Tables: ConvertTables(fromSchema.Tables),
		}
		to = append(to, toSchema)
	}

	return to
}

func ConvertTables(from []*db.Table) []*Table {
	to := []*Table{}
	for _, fromTable := range from {
		toTable := &Table{
			Name:                     fromTable.Name,
			TotalBytes:               fromTable.TotalBytes,
			TotalBytesTotal:          fromTable.TotalBytesTotal,
			IndexBytes:               fromTable.IndexBytes,
			IndexBytesTotal:          fromTable.IndexBytesTotal,
			ToastBytes:               fromTable.ToastBytes,
			ToastBytesTotal:          fromTable.ToastBytesTotal,
			TableBytes:               fromTable.TableBytes,
			TableBytesTotal:          fromTable.TableBytesTotal,
			BloatBytes:               fromTable.BloatBytes,
			BloatBytesTotal:          fromTable.BloatBytesTotal,
			BloatFactor:              fromTable.BloatFactor,
			SequentialScans:          fromTable.SequentialScans,
			SequentialScanReadRows:   fromTable.SequentialScanReadRows,
			IndexScans:               fromTable.IndexScans,
			IndexScanReadRows:        fromTable.IndexScanReadRows,
			InsertedRows:             fromTable.InsertedRows,
			UpdatedRows:              fromTable.UpdatedRows,
			DeletedRows:              fromTable.DeletedRows,
			LiveRowEstimate:          fromTable.LiveRowEstimate,
			LiveRowEstimateTotal:     fromTable.LiveRowEstimateTotal,
			DeadRowEstimate:          fromTable.DeadRowEstimate,
			DeadRowEstimateTotal:     fromTable.DeadRowEstimateTotal,
			ModifiedRowsSinceAnalyze: fromTable.ModifiedRowsSinceAnalyze,
			LastVacuumAt:             convertSqlNullInt64(fromTable.LastVacuumAt),
			LastAutovacuumAt:         convertSqlNullInt64(fromTable.LastAutovacuumAt),
			LastAnalyzeAt:            convertSqlNullInt64(fromTable.LastAnalyzeAt),
			LastAutoanalyzeAt:        convertSqlNullInt64(fromTable.LastAutoanalyzeAt),
			VacuumCount:              fromTable.VacuumCount,
			AutovacuumCount:          fromTable.AutovacuumCount,
			AnalyzeCount:             fromTable.AnalyzeCount,
			AutoanalyzeCount:         fromTable.AutoanalyzeCount,
			DiskBlocksRead:           fromTable.DiskBlocksRead,
			DiskBlocksHit:            fromTable.DiskBlocksHit,
			DiskBlocksHitPercent:     fromTable.DiskBlocksHitPercent,
			DiskIndexBlocksRead:      fromTable.DiskIndexBlocksRead,
			DiskIndexBlocksHit:       fromTable.DiskIndexBlocksHit,
			DiskToastBlocksRead:      fromTable.DiskToastBlocksRead,
			DiskToastBlocksHit:       fromTable.DiskToastBlocksHit,
			DiskToastIndexBlocksRead: fromTable.DiskToastIndexBlocksRead,
			DiskToastIndexBlocksHit:  fromTable.DiskToastIndexBlocksHit,
			Columns:                  ConvertColumns(fromTable.Columns),
			Indexes:                  ConvertIndexes(fromTable.Indexes),
		}
		to = append(to, toTable)
	}
	return to
}

func ConvertColumns(from []*db.Column) []*Column {
	to := []*Column{}
	for _, fromColumn := range from {
		toColumn := &Column{
			Name:             fromColumn.Name,
			Default:          fromColumn.Default.String,
			Type:             fromColumn.Type,
			Nullable:         convertSqlYesNoToBool(fromColumn.Nullable),
			MaxLength:        int(convertSqlNullInt64(fromColumn.MaxLength)),
			NumericPrecision: int(convertSqlNullInt64(fromColumn.NumericPrecision)),
			NumericScale:     int(convertSqlNullInt64(fromColumn.NumericScale)),
			IntervalType:     fromColumn.IntervalType.String,
			IsIdentity:       convertSqlYesNoToBool(fromColumn.IsIdentity),
		}
		to = append(to, toColumn)
	}
	return to
}

func ConvertIndexes(from []*db.Index) []*Index {
	to := []*Index{}
	for _, fromIndex := range from {
		toIndex := &Index{
			Name:            fromIndex.Name,
			Unique:          fromIndex.Unique,
			Unused:          fromIndex.Unused,
			Valid:           fromIndex.Valid,
			Bytes:           fromIndex.Bytes,
			BytesTotal:      fromIndex.BytesTotal,
			BloatBytes:      fromIndex.BloatBytes,
			BloatBytesTotal: fromIndex.BloatBytesTotal,
			BloatFactor:     fromIndex.BloatFactor,
			Scans:           fromIndex.Scans,
			DiskBlocksRead:  fromIndex.DiskBlocksRead,
			DiskBlocksHit:   fromIndex.DiskBlocksHit,
			Definition:      fromIndex.Definition,
		}
		to = append(to, toIndex)
	}
	return to
}

func ConvertReplica(from *db.Replica) *Replica {
	if from == nil {
		return nil // return nil to not send replica
	}

	return &Replica{
		ApplicationName:   from.ApplicationName,
		PrimaryConfigName: from.PrimaryConfigName,
		Status:            from.Status,
	}
}

func ConvertReplicas(from []*db.ReplicaClient) []*ReplicaClient {
	to := []*ReplicaClient{}
	for _, fromReplicaClient := range from {
		var toReplicaClient ReplicaClient
		toReplicaClient.ApplicationName = fromReplicaClient.ApplicationName
		if fromReplicaClient.BackendStart.Valid {
			toReplicaClient.BackendStart = fromReplicaClient.BackendStart.Time.Unix()
		}
		if fromReplicaClient.Backendxmin.Valid {
			toReplicaClient.Backendxmin = fromReplicaClient.Backendxmin.Int64
		}
		if fromReplicaClient.State.Valid {
			toReplicaClient.State = fromReplicaClient.State.String
		}
		if fromReplicaClient.SyncPriority.Valid {
			toReplicaClient.SyncPriority = int(fromReplicaClient.SyncPriority.Int32)
		}
		if fromReplicaClient.SyncState.Valid {
			toReplicaClient.SyncState = fromReplicaClient.SyncState.String
		}
		to = append(to, &toReplicaClient)
	}
	return to
}

func ConvertSettings(from []db.Setting, fromServer db.PostgresServer) []*Setting {
	to := []*Setting{}

	for _, fromSetting := range from {
		// filter to settings on the same server
		if fromSetting.ServerID.Name == fromServer.ServerID.Name {
			toSetting := &Setting{
				Name:           fromSetting.Name,
				Value:          fromSetting.Value,
				Category:       fromSetting.Category,
				Description:    fromSetting.Description,
				Context:        fromSetting.Context,
				VarType:        fromSetting.VarType,
				Source:         fromSetting.Source,
				PendingRestart: fromSetting.PendingRestart,
			}
			if fromSetting.Unit.Valid {
				toSetting.Unit = fromSetting.Unit.String
			}
			if fromSetting.MinVal.Valid {
				toSetting.MinVal = fromSetting.MinVal.String
			}
			if fromSetting.MaxVal.Valid {
				toSetting.MaxVal = fromSetting.MaxVal.String
			}
			if fromSetting.EnumVals.Valid {
				toSetting.EnumVals = fromSetting.EnumVals.String
			}
			if fromSetting.BootVal.Valid {
				toSetting.BootVal = fromSetting.BootVal.String
			}
			if fromSetting.ResetVal.Valid {
				toSetting.ResetVal = fromSetting.ResetVal.String
			}
			to = append(to, toSetting)
		}
	}

	return to
}

func ConvertStats(stats *util.Stats) *Stats {
	data := stats.ToMap()

	if len(data) == 0 {
		return nil
	}

	return &Stats{
		LogStats: ConvertLogStats(data),
	}
}

func ConvertLogStats(stats map[string]int) *LogStats {
	if len(stats) == 0 {
		return nil
	}

	return &LogStats{
		Received:            stats["logs.received"],
		Postgres:            stats["logs.postgres"],
		Handled:             stats["logs.handled"],
		MetricLines:         stats["logs.metric_lines"],
		MetricsLinesDropped: stats["logs.metric_lines.dropped"],
		SlowQueries:         stats["logs.slow_queries"],
		SlowQueriesDropped:  stats["logs.slow_queries.dropped"],
	}
}

func ConvertErrors(errors []errors.ErrorReport) []*Error {
	if len(errors) == 0 {
		return nil
	}

	var toErrors []*Error

	for _, err := range errors {
		toErr := &Error{
			Error:      err.Error.Error(),
			Panic:      err.Panic,
			StackTrace: err.StackTrace,
		}
		toErrors = append(toErrors, toErr)
	}

	return toErrors
}

func convertSqlNullInt64(nullInt64 sql.NullInt64) int64 {
	if nullInt64.Valid {
		return nullInt64.Int64
	} else {
		return 0
	}
}

func convertSqlYesNoToBool(yesNo sql.NullString) bool {
	return yesNo.String == "YES"
}
