package db

import (
	"agent/errors"
	"agent/logger"
	"agent/util"
	"database/sql"
	"log"
)

// stateful stats object that stores all database schema per server id
// we calculate delta and total metrics for tables and indexes with it
type DatabaseSchemaState struct {
	// map of server config name + database to database/schema
	Databases map[ServerID]*Database
}

type Database struct {
	ServerID *ServerID
	Name     string
	Schemas  []*Schema
}

type Schema struct {
	Name   string
	Tables []*Table
}

type Table struct {
	Name            string
	Schema          string
	TotalBytes      int64
	TotalBytesTotal int64
	IndexBytes      int64
	IndexBytesTotal int64
	ToastBytes      int64
	ToastBytesTotal int64
	TableBytes      int64
	TableBytesTotal int64
	BloatBytes      int64
	BloatBytesTotal int64
	BloatFactor     float64

	// stats
	SequentialScans          int64
	SequentialScanReadRows   int64
	IndexScans               int64
	IndexScanReadRows        int64
	InsertedRows             int64
	UpdatedRows              int64
	DeletedRows              int64
	LiveRowEstimate          int64
	LiveRowEstimateTotal     int64
	DeadRowEstimate          int64
	DeadRowEstimateTotal     int64
	ModifiedRowsSinceAnalyze int64
	LastVacuumAt             sql.NullInt64
	LastAutovacuumAt         sql.NullInt64
	LastAnalyzeAt            sql.NullInt64
	LastAutoanalyzeAt        sql.NullInt64
	VacuumCount              int64
	AutovacuumCount          int64
	AnalyzeCount             int64
	AutoanalyzeCount         int64
	DiskBlocksRead           int64
	DiskBlocksHit            int64
	DiskBlocksHitPercent     float64
	DiskIndexBlocksRead      int64
	DiskIndexBlocksHit       int64
	DiskToastBlocksRead      int64
	DiskToastBlocksHit       int64
	DiskToastIndexBlocksRead int64
	DiskToastIndexBlocksHit  int64

	Columns []*Column
	Indexes []*Index
}

type Column struct {
	Schema           string
	TableName        string
	Name             string
	Default          sql.NullString
	Type             string
	Nullable         sql.NullString // YES or NO
	MaxLength        sql.NullInt64
	NumericPrecision sql.NullInt64
	NumericScale     sql.NullInt64
	IntervalType     sql.NullString
	IsIdentity       sql.NullString // YES or NO
}

type Index struct {
	Name            string
	Schema          string
	TableName       string
	Unique          bool
	Unused          bool
	Valid           bool
	Definition      string
	Bytes           int64
	BytesTotal      int64
	BloatBytes      int64
	BloatBytesTotal int64
	BloatFactor     float64
	Scans           int64
	DiskBlocksRead  int64
	DiskBlocksHit   int64
}

type UnusedIndex struct {
	Name      string
	Schema    string
	TableName string
}

func (t *Table) Delta(latest *Table) *Table {
	table := &Table{
		Name:                     latest.Name,
		Schema:                   latest.Schema,
		TotalBytes:               latest.TotalBytesTotal - t.TotalBytesTotal,
		TotalBytesTotal:          latest.TotalBytesTotal,
		IndexBytes:               latest.IndexBytesTotal - t.IndexBytesTotal,
		IndexBytesTotal:          latest.IndexBytesTotal,
		ToastBytes:               latest.ToastBytesTotal - t.ToastBytesTotal,
		ToastBytesTotal:          latest.ToastBytesTotal,
		TableBytes:               latest.TableBytesTotal - t.TableBytesTotal,
		TableBytesTotal:          latest.TableBytesTotal,
		BloatBytesTotal:          latest.BloatBytesTotal,
		BloatFactor:              latest.BloatFactor,
		SequentialScans:          latest.SequentialScans - t.SequentialScans,
		SequentialScanReadRows:   latest.SequentialScanReadRows - t.SequentialScanReadRows,
		IndexScans:               latest.IndexScans - t.IndexScans,
		IndexScanReadRows:        latest.IndexScanReadRows - t.IndexScanReadRows,
		InsertedRows:             latest.InsertedRows - t.InsertedRows,
		UpdatedRows:              latest.UpdatedRows - t.UpdatedRows,
		DeletedRows:              latest.DeletedRows - t.DeletedRows,
		LiveRowEstimate:          latest.LiveRowEstimateTotal - t.LiveRowEstimateTotal,
		LiveRowEstimateTotal:     latest.LiveRowEstimateTotal,
		DeadRowEstimateTotal:     latest.DeadRowEstimateTotal,
		ModifiedRowsSinceAnalyze: latest.ModifiedRowsSinceAnalyze,
		LastVacuumAt:             latest.LastVacuumAt,
		LastAutovacuumAt:         latest.LastAutovacuumAt,
		LastAnalyzeAt:            latest.LastAnalyzeAt,
		LastAutoanalyzeAt:        latest.LastAutoanalyzeAt,
		VacuumCount:              latest.VacuumCount - t.VacuumCount,
		AutovacuumCount:          latest.AutovacuumCount - t.AutovacuumCount,
		AnalyzeCount:             latest.AnalyzeCount - t.AnalyzeCount,
		AutoanalyzeCount:         latest.AutoanalyzeCount - t.AutoanalyzeCount,
		DiskBlocksRead:           latest.DiskBlocksRead - t.DiskBlocksRead,
		DiskBlocksHit:            latest.DiskBlocksHit - t.DiskBlocksHit,
		DiskIndexBlocksRead:      latest.DiskIndexBlocksRead - t.DiskIndexBlocksRead,
		DiskIndexBlocksHit:       latest.DiskIndexBlocksHit - t.DiskIndexBlocksHit,
		DiskToastBlocksRead:      latest.DiskToastBlocksRead - t.DiskToastBlocksRead,
		DiskToastBlocksHit:       latest.DiskToastBlocksHit - t.DiskToastBlocksHit,
		DiskToastIndexBlocksRead: latest.DiskToastIndexBlocksRead - t.DiskToastIndexBlocksRead,
		DiskToastIndexBlocksHit:  latest.DiskToastIndexBlocksHit - t.DiskToastIndexBlocksHit,

		// we delta tables before columns/indexes are set so these are not really needed
		Columns: latest.Columns,
		Indexes: latest.Indexes, // we delta indexes below
	}
	// dead row estimate, and bloat bytes can be negative which doesn't make much sense
	deadRowEstimate := latest.DeadRowEstimateTotal - t.DeadRowEstimateTotal
	if deadRowEstimate > 0 {
		table.DeadRowEstimate = deadRowEstimate
	}
	bloatBytes := latest.BloatBytesTotal - t.BloatBytesTotal
	if bloatBytes > 0 {
		table.BloatBytes = bloatBytes
	}
	table.DiskBlocksHitPercent = util.HitPercent(float64(table.DiskBlocksHit), float64(table.DiskBlocksRead))
	return table
}

func (i *Index) Delta(latest *Index) *Index {
	index := &Index{
		Name:            latest.Name,
		Schema:          latest.Schema,
		TableName:       latest.TableName,
		Unique:          latest.Unique,
		Unused:          latest.Unused,
		Valid:           latest.Valid,
		Definition:      latest.Definition,
		Bytes:           latest.BytesTotal - i.BytesTotal,
		BytesTotal:      latest.BytesTotal,
		BloatBytesTotal: latest.BloatBytesTotal,
		BloatFactor:     latest.BloatFactor,
		Scans:           latest.Scans - i.Scans,
		DiskBlocksRead:  latest.DiskBlocksRead - i.DiskBlocksRead,
		DiskBlocksHit:   latest.DiskBlocksHit - i.DiskBlocksHit,
	}
	// bloat values can be negative if bloat is reduced
	bloatBytes := latest.BloatBytesTotal - i.BloatBytesTotal
	if bloatBytes > 0 {
		index.BloatBytes = bloatBytes
	}
	return index
}

func (o *Observer) MonitorSchemas() {
	for _, postgresClient := range o.postgresClients {
		go NewMonitorWorker(
			o.config,
			postgresClient,
			&SchemaMonitor{
				schemaChannel:       o.schemaChannel,
				databaseSchemaState: o.databaseSchemaState,
			},
		).Start()
	}
}

type SchemaMonitor struct {
	schemaChannel       chan *Database
	databaseSchemaState *DatabaseSchemaState
}

func (m *SchemaMonitor) Run(postgresClient *PostgresClient) {
	// initialize state object
	if m.databaseSchemaState.Databases == nil {
		m.databaseSchemaState.Databases = make(map[ServerID]*Database)
	}

	schemas := m.FindSchemas(postgresClient)
	tables := m.FindTables(postgresClient)
	indexes := m.FindIndexes(postgresClient)
	bloat := m.FindBloat(postgresClient)

	// ordering matters with these
	// add tables to schemas
	for _, table := range tables {
		for _, schema := range schemas {
			if schema.Name == table.Schema {
				schema.Tables = append(schema.Tables, table)
			}
		}
	}

	// add indexes to tables
	for _, index := range indexes {
		for _, schema := range schemas {
			if index.Schema == schema.Name {
				for _, table := range schema.Tables {
					if index.TableName == table.Name {
						table.Indexes = append(table.Indexes, index)
					}
				}
			}
		}
	}

	// add bloat to tables/indexes
	for _, b := range bloat {
		if b.Type == "table" {
			for _, table := range tables {
				if table.Schema == b.Schemaname && table.Name == b.Name {
					table.BloatBytesTotal = b.Waste
					table.BloatFactor = b.Bloat
				}
			}
		} else if b.Type == "index" {
			for _, index := range indexes {
				if index.Schema == b.Schemaname && index.Name == b.Name {
					index.BloatBytesTotal = b.Waste
					index.BloatFactor = b.Bloat
				}
			}
		}
	}

	// database contains delta tables and indexes
	currentDatabase := &Database{
		ServerID: postgresClient.serverID,
		Name:     postgresClient.serverID.Database,
		Schemas:  schemas,
	}

	var deltaDatabase *Database

	// delta tables and indexes after stitching objects together to make sure bloat and other metrics are set
	previousDatabase, ok := m.databaseSchemaState.Databases[*postgresClient.serverID]
	if ok {
		var deltaSchemas []*Schema
		for _, schema := range schemas {
			deltaSchema := &Schema{
				Name:   schema.Name,
				Tables: m.deltaTables(schema.Tables, previousDatabase),
			}
			deltaSchemas = append(deltaSchemas, deltaSchema)
		}
		// create new delta database to keep table and index fields 0'd out for next polling interval
		// else we end up with sawtooth data
		deltaDatabase = &Database{
			ServerID: postgresClient.serverID,
			Name:     postgresClient.serverID.Database,
			Schemas:  deltaSchemas,
		}
	}

	// always save the latest database for next polling interval
	m.databaseSchemaState.Databases[*postgresClient.serverID] = currentDatabase

	// only report the database schemas if we've had two poll intervals
	// since the first iteration won't have the correct deltas
	if previousDatabase != nil {
		select {
		case m.schemaChannel <- deltaDatabase:
			// sent
		default:
			logger.Warn("Dropping schema database: channel buffer full")
		}
	}
}

func (m *SchemaMonitor) deltaTables(tables []*Table, previousDatabase *Database) []*Table {
	var deltaTables []*Table

	// for tables, find previous instance and delta them
	// total fields will get reported the first polling interval but
	// delta fields will get reported after two polling intervals
	for _, table := range tables {
		for _, previousSchema := range previousDatabase.Schemas {
			if table.Schema == previousSchema.Name {
				for _, previousTable := range previousSchema.Tables {
					if table.Name == previousTable.Name {
						delta := previousTable.Delta(table)
						deltaTables = append(deltaTables, delta)

						delta.Indexes = m.deltaIndexes(table.Indexes, previousDatabase)
					}
				}
			}
		}
	}

	return deltaTables
}

func (m *SchemaMonitor) deltaIndexes(indexes []*Index, previousDatabase *Database) []*Index {
	var deltaIndexes []*Index

	// for indexes, find previous instance and delta them
	// total fields will get reported the first polling interval but
	// delta fields will get reported after two polling intervals
	for _, index := range indexes {
		for _, previousSchema := range previousDatabase.Schemas {
			if index.Schema == previousSchema.Name {
				for _, previousTable := range previousSchema.Tables {
					if index.TableName == previousTable.Name {
						for _, previousIndex := range previousTable.Indexes {
							if index.Name == previousIndex.Name {
								delta := previousIndex.Delta(index)
								deltaIndexes = append(deltaIndexes, delta)
							}
						}
					}
				}
			}
		}
	}

	return deltaIndexes
}

func (m *SchemaMonitor) FindSchemas(postgresClient *PostgresClient) []*Schema {
	query := `select schema_name as name from information_schema.schemata
						where schema_name not in ('pg_catalog', 'information_schema', 'pg_toast', 'heroku_ext')
						and schema_name not like 'pg_toast_temp_%' and schema_name not like 'pg_temp_%'` + postgresMonitorQueryComment()
	var schemas []*Schema

	rows, err := postgresClient.client.Query(query)

	if err != nil {
		return []*Schema{}
	}
	defer rows.Close()

	for rows.Next() {
		var schema Schema
		err := rows.Scan(&schema.Name)
		if err != nil {
			continue
		}
		schemas = append(schemas, &schema)
	}

	return schemas
}

func (m *SchemaMonitor) FindTables(postgresClient *PostgresClient) []*Table {
	query := `select *, total_bytes - index_bytes - coalesce(toast_bytes, 0) as table_bytes from (
							select pgc.relname as name,
										 pgn.nspname as schema,
										 coalesce(pg_total_relation_size(pgc.oid), 0) as total_bytes,
										 coalesce(pg_indexes_size(pgc.oid), 0) as index_bytes,
										 coalesce(pg_total_relation_size(reltoastrelid), 0) as toast_bytes
								from pg_class pgc
								left join pg_namespace pgn on pgn.oid = pgc.relnamespace
								where relkind = 'r'
								and nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'heroku_ext')
						) s` + postgresMonitorQueryComment()
	var tables []*Table
	rows, err := postgresClient.client.Query(query)
	if err != nil {
		return []*Table{}
	}
	defer rows.Close()

	for rows.Next() {
		var table Table
		err := rows.Scan(
			&table.Name,
			&table.Schema,
			&table.TotalBytesTotal,
			&table.IndexBytesTotal,
			&table.ToastBytesTotal,
			&table.TableBytesTotal,
		)
		if err != nil {
			continue
		}
		tables = append(tables, &table)
	}

	// merge in table columns
	columns := m.FindTableColumns(postgresClient)
	for _, column := range columns {
		for _, table := range tables {
			if table.Schema == column.Schema && table.Name == column.TableName {
				table.Columns = append(table.Columns, column)
			}
		}
	}

	// merge in table stats
	tableStats := m.FindTableStats(postgresClient)
	for _, tableStat := range tableStats {
		for _, table := range tables {
			if table.Schema == tableStat.Schema && table.Name == tableStat.Name {
				table.SequentialScans = tableStat.SequentialScans
				table.SequentialScanReadRows = tableStat.SequentialScanReadRows
				table.IndexScans = tableStat.IndexScans
				table.IndexScanReadRows = tableStat.IndexScanReadRows
				table.InsertedRows = tableStat.InsertedRows
				table.UpdatedRows = tableStat.UpdatedRows
				table.DeletedRows = tableStat.DeletedRows
				table.LiveRowEstimateTotal = tableStat.LiveRowEstimateTotal
				table.DeadRowEstimateTotal = tableStat.DeadRowEstimateTotal
				table.ModifiedRowsSinceAnalyze = tableStat.ModifiedRowsSinceAnalyze
				table.LastVacuumAt = tableStat.LastVacuumAt
				table.LastAutovacuumAt = tableStat.LastAutovacuumAt
				table.LastAnalyzeAt = tableStat.LastAnalyzeAt
				table.LastAutoanalyzeAt = tableStat.LastAutoanalyzeAt
				table.VacuumCount = tableStat.VacuumCount
				table.AutovacuumCount = tableStat.AutovacuumCount
				table.AnalyzeCount = tableStat.AnalyzeCount
				table.AutoanalyzeCount = tableStat.AutoanalyzeCount
				table.DiskBlocksRead = tableStat.DiskBlocksRead
				table.DiskBlocksHit = tableStat.DiskBlocksHit
				table.DiskIndexBlocksRead = tableStat.DiskIndexBlocksRead
				table.DiskIndexBlocksHit = tableStat.DiskIndexBlocksHit
				table.DiskToastBlocksRead = tableStat.DiskToastBlocksRead
				table.DiskToastBlocksHit = tableStat.DiskToastBlocksHit
				table.DiskToastIndexBlocksRead = tableStat.DiskToastIndexBlocksRead
				table.DiskToastIndexBlocksHit = tableStat.DiskToastIndexBlocksHit

				break
			}
		}
	}

	return tables
}

func (m *SchemaMonitor) FindTableColumns(postgresClient *PostgresClient) []*Column {
	// using view definition from \d+ information_schema.columns without the role checks
	// to ensure read only users can access this data
	query := `SELECT table_schema, table_name, column_name, column_default, is_nullable,
						data_type, character_maximum_length, numeric_precision, numeric_scale, interval_type, is_identity FROM
							(SELECT current_database()::information_schema.sql_identifier AS table_catalog,
							nc.nspname::information_schema.sql_identifier AS table_schema,
							c.relname::information_schema.sql_identifier AS table_name,
							a.attname::information_schema.sql_identifier AS column_name,
							a.attnum::information_schema.cardinal_number AS ordinal_position,
							CASE
									WHEN a.attgenerated = ''::"char" THEN pg_get_expr(ad.adbin, ad.adrelid)
									ELSE NULL::text
							END::information_schema.character_data AS column_default,
							CASE
									WHEN a.attnotnull OR t.typtype = 'd'::"char" AND t.typnotnull THEN 'NO'::text
									ELSE 'YES'::text
							END::information_schema.yes_or_no AS is_nullable,
							CASE
									WHEN t.typtype = 'd'::"char" THEN
									CASE
											WHEN bt.typelem <> 0::oid AND bt.typlen = '-1'::integer THEN 'ARRAY'::text
											WHEN nbt.nspname = 'pg_catalog'::name THEN format_type(t.typbasetype, NULL::integer)
											ELSE 'USER-DEFINED'::text
									END
									ELSE
									CASE
											WHEN t.typelem <> 0::oid AND t.typlen = '-1'::integer THEN 'ARRAY'::text
											WHEN nt.nspname = 'pg_catalog'::name THEN format_type(a.atttypid, NULL::integer)
											ELSE 'USER-DEFINED'::text
									END
							END::information_schema.character_data AS data_type,
							information_schema._pg_char_max_length(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS character_maximum_length,
							information_schema._pg_char_octet_length(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS character_octet_length,
							information_schema._pg_numeric_precision(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS numeric_precision,
							information_schema._pg_numeric_precision_radix(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS numeric_precision_radix,
							information_schema._pg_numeric_scale(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS numeric_scale,
							information_schema._pg_datetime_precision(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS datetime_precision,
							information_schema._pg_interval_type(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*))::information_schema.character_data AS interval_type,
							NULL::integer::information_schema.cardinal_number AS interval_precision,
							NULL::name::information_schema.sql_identifier AS character_set_catalog,
							NULL::name::information_schema.sql_identifier AS character_set_schema,
							NULL::name::information_schema.sql_identifier AS character_set_name,
							CASE
									WHEN nco.nspname IS NOT NULL THEN current_database()
									ELSE NULL::name
							END::information_schema.sql_identifier AS collation_catalog,
							nco.nspname::information_schema.sql_identifier AS collation_schema,
							co.collname::information_schema.sql_identifier AS collation_name,
							CASE
									WHEN t.typtype = 'd'::"char" THEN current_database()
									ELSE NULL::name
							END::information_schema.sql_identifier AS domain_catalog,
							CASE
									WHEN t.typtype = 'd'::"char" THEN nt.nspname
									ELSE NULL::name
							END::information_schema.sql_identifier AS domain_schema,
							CASE
									WHEN t.typtype = 'd'::"char" THEN t.typname
									ELSE NULL::name
							END::information_schema.sql_identifier AS domain_name,
							current_database()::information_schema.sql_identifier AS udt_catalog,
							COALESCE(nbt.nspname, nt.nspname)::information_schema.sql_identifier AS udt_schema,
							COALESCE(bt.typname, t.typname)::information_schema.sql_identifier AS udt_name,
							NULL::name::information_schema.sql_identifier AS scope_catalog,
							NULL::name::information_schema.sql_identifier AS scope_schema,
							NULL::name::information_schema.sql_identifier AS scope_name,
							NULL::integer::information_schema.cardinal_number AS maximum_cardinality,
							a.attnum::information_schema.sql_identifier AS dtd_identifier,
							'NO'::character varying::information_schema.yes_or_no AS is_self_referencing,
							CASE
									WHEN a.attidentity = ANY (ARRAY['a'::"char", 'd'::"char"]) THEN 'YES'::text
									ELSE 'NO'::text
							END::information_schema.yes_or_no AS is_identity,
							CASE a.attidentity
									WHEN 'a'::"char" THEN 'ALWAYS'::text
									WHEN 'd'::"char" THEN 'BY DEFAULT'::text
									ELSE NULL::text
							END::information_schema.character_data AS identity_generation,
							seq.seqstart::information_schema.character_data AS identity_start,
							seq.seqincrement::information_schema.character_data AS identity_increment,
							seq.seqmax::information_schema.character_data AS identity_maximum,
							seq.seqmin::information_schema.character_data AS identity_minimum,
							CASE
									WHEN seq.seqcycle THEN 'YES'::text
									ELSE 'NO'::text
							END::information_schema.yes_or_no AS identity_cycle,
							CASE
									WHEN a.attgenerated <> ''::"char" THEN 'ALWAYS'::text
									ELSE 'NEVER'::text
							END::information_schema.character_data AS is_generated,
							CASE
									WHEN a.attgenerated <> ''::"char" THEN pg_get_expr(ad.adbin, ad.adrelid)
									ELSE NULL::text
							END::information_schema.character_data AS generation_expression,
							CASE
									WHEN (c.relkind = ANY (ARRAY['r'::"char", 'p'::"char"])) OR (c.relkind = ANY (ARRAY['v'::"char", 'f'::"char"])) AND pg_column_is_updatable(c.oid::regclass, a.attnum, false) THEN 'YES'::text
									ELSE 'NO'::text
							END::information_schema.yes_or_no AS is_updatable
							FROM pg_attribute a
							LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
							JOIN (pg_class c
							JOIN pg_namespace nc ON c.relnamespace = nc.oid) ON a.attrelid = c.oid
							JOIN (pg_type t
							JOIN pg_namespace nt ON t.typnamespace = nt.oid) ON a.atttypid = t.oid
							LEFT JOIN (pg_type bt
							JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid) ON t.typtype = 'd'::"char" AND t.typbasetype = bt.oid
							LEFT JOIN (pg_collation co
							JOIN pg_namespace nco ON co.collnamespace = nco.oid) ON a.attcollation = co.oid AND (nco.nspname <> 'pg_catalog'::name OR co.collname <> 'default'::name)
							LEFT JOIN (pg_depend dep
							JOIN pg_sequence seq ON dep.classid = 'pg_class'::regclass::oid AND dep.objid = seq.seqrelid AND dep.deptype = 'i'::"char") ON dep.refclassid = 'pg_class'::regclass::oid AND dep.refobjid = c.oid AND dep.refobjsubid = a.attnum
							WHERE NOT pg_is_other_temp_schema(nc.oid) AND a.attnum > 0 AND NOT a.attisdropped AND (c.relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'f'::"char", 'p'::"char"]))) as columns
						WHERE columns.table_catalog = current_database()
						AND columns.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'heroku_ext')
						ORDER BY columns.table_name ASC, columns.column_name ASC` + postgresMonitorQueryComment()
	var columns []*Column
	rows, err := postgresClient.client.Query(query)
	if err != nil {
		logger.Error("Find table columns error", "err", err)
		errors.Report(err)
		return []*Column{}
	}
	defer rows.Close()

	for rows.Next() {
		var column Column
		err := rows.Scan(
			&column.Schema,
			&column.TableName,
			&column.Name,
			&column.Default,
			&column.Nullable,
			&column.Type,
			&column.MaxLength,
			&column.NumericPrecision,
			&column.NumericScale,
			&column.IntervalType,
			&column.IsIdentity,
		)
		if err != nil {
			logger.Error("Find table columns error", "err", err)
			errors.Report(err)
			continue
		}
		if column.Type != "numeric" {
			column.NumericPrecision = sql.NullInt64{Valid: false, Int64: 0}
			column.NumericScale = sql.NullInt64{Valid: false, Int64: 0}
		}

		columns = append(columns, &column)
	}

	return columns
}

func (m *SchemaMonitor) FindTableStats(postgresClient *PostgresClient) []*Table {
	query := `select stat.relname as name, stat.schemaname as schema, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch,
						n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup, n_mod_since_analyze,
						extract(epoch from last_vacuum)::int as last_vacuum,
						extract(epoch from last_autovacuum)::int as last_autovacuum,
						extract(epoch from last_analyze)::int as last_analyze,
						extract(epoch from last_autoanalyze)::int as last_autoanalyze,
						vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
						heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit, toast_blks_read, toast_blks_hit,
						tidx_blks_read, tidx_blks_hit
						from pg_stat_user_tables stat
						join pg_statio_user_tables statio on statio.relid = stat.relid
						where stat.schemaname not in ('pg_catalog', 'information_schema', 'pg_toast', 'heroku_ext')` + postgresMonitorQueryComment()
	var tables []*Table
	rows, err := postgresClient.client.Query(query)
	if err != nil {
		logger.Error("Find table stats error", "err", err)
		errors.Report(err)
		return []*Table{}
	}
	defer rows.Close()

	for rows.Next() {
		var table Table

		var diskToastBlocksRead sql.NullInt64
		var diskToastBlocksHit sql.NullInt64
		var diskToastIndexBlocksRead sql.NullInt64
		var diskToastIndexBlocksHit sql.NullInt64

		err := rows.Scan(
			&table.Name,
			&table.Schema,
			&table.SequentialScans,
			&table.SequentialScanReadRows,
			&table.IndexScans,
			&table.IndexScanReadRows,
			&table.InsertedRows,
			&table.UpdatedRows,
			&table.DeletedRows,
			&table.LiveRowEstimateTotal,
			&table.DeadRowEstimateTotal,
			&table.ModifiedRowsSinceAnalyze,
			&table.LastVacuumAt,
			&table.LastAutovacuumAt,
			&table.LastAnalyzeAt,
			&table.LastAutoanalyzeAt,
			&table.VacuumCount,
			&table.AutovacuumCount,
			&table.AnalyzeCount,
			&table.AutoanalyzeCount,
			&table.DiskBlocksRead,
			&table.DiskBlocksHit,
			&table.DiskIndexBlocksRead,
			&table.DiskIndexBlocksHit,
			&diskToastBlocksRead,
			&diskToastBlocksHit,
			&diskToastIndexBlocksRead,
			&diskToastIndexBlocksHit,
		)
		if err != nil {
			logger.Error("Find table stats error", "err", err)
			errors.Report(err)
			continue
		}
		if diskToastBlocksRead.Valid {
			table.DiskToastBlocksRead = diskToastBlocksRead.Int64
		}
		if diskToastBlocksHit.Valid {
			table.DiskToastBlocksHit = diskToastBlocksHit.Int64
		}
		if diskToastIndexBlocksRead.Valid {
			table.DiskToastIndexBlocksRead = diskToastIndexBlocksRead.Int64
		}
		if diskToastIndexBlocksHit.Valid {
			table.DiskToastIndexBlocksHit = diskToastIndexBlocksHit.Int64
		}
		tables = append(tables, &table)
	}

	return tables
}

func (m *SchemaMonitor) FindIndexes(postgresClient *PostgresClient) []*Index {
	query := `select idx.relname as name,
							nsp.nspname as schema,
							tbl.relname as table_name,
							pgi.indisunique as unique,
							pgi.indisvalid as valid,
							pg_relation_size(idx.oid) as bytes,
							istat.idx_scan as scans,
							idx_blks_read as blocks_read,
							idx_blks_hit as blocks_hit,
							pgis.indexdef as definition
						from pg_index pgi
							join pg_class idx on idx.oid = pgi.indexrelid
							join pg_namespace nsp on nsp.oid = idx.relnamespace
							join pg_class tbl on tbl.oid = pgi.indrelid
							join pg_namespace tnsp on tnsp.oid = tbl.relnamespace
							join pg_stat_user_indexes istat on istat.indexrelid = pgi.indexrelid
							join pg_statio_user_indexes istatio on istatio.indexrelid = pgi.indexrelid
							join pg_indexes pgis on pgis.indexname = idx.relname
						where tnsp.nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'heroku_ext')` + postgresMonitorQueryComment()

	var indexes []*Index
	rows, err := postgresClient.client.Query(query)
	if err != nil {
		return []*Index{}
	}
	defer rows.Close()

	for rows.Next() {
		var index Index
		err := rows.Scan(
			&index.Name,
			&index.Schema,
			&index.TableName,
			&index.Unique,
			&index.Valid,
			&index.BytesTotal,
			&index.Scans,
			&index.DiskBlocksRead,
			&index.DiskBlocksHit,
			&index.Definition,
		)
		if err != nil {
			logger.Error("Index error", "err", err)
			errors.Report(err)
			continue
		}

		indexes = append(indexes, &index)
	}

	if err := rows.Err(); err != nil {
		logger.Error("Indexes error", "err", err)
		errors.Report(err)
	}

	// add unused field
	unusedIndexes := m.FindUnusedIndexes(postgresClient)

	for _, unusedIndex := range unusedIndexes {
		for _, index := range indexes {
			if index.Name == unusedIndex.Name && index.Schema == unusedIndex.Schema {
				index.Unused = true
			}
		}
	}

	return indexes
}

// not directly using index scan count == 0 for unused indexes since an index
// could be unique or used in a constraint / expression as well
func (m *SchemaMonitor) FindUnusedIndexes(postgresClient *PostgresClient) []*UnusedIndex {
	unusedIndexesQuery := `SELECT s.indexrelname AS indexname,
													s.schemaname,
													s.relname AS tablename
												FROM pg_catalog.pg_stat_user_indexes s
												JOIN pg_catalog.pg_index i ON s.indexrelid = i.indexrelid
												WHERE s.idx_scan = 0    -- has never been scanned
												AND 0 <>ALL (i.indkey)  -- no index column is an expression
												AND NOT i.indisunique   -- is not a UNIQUE index
												AND NOT EXISTS          -- does not enforce a constraint
														(SELECT 1 FROM pg_catalog.pg_constraint c
															WHERE c.conindid = s.indexrelid)
												ORDER BY tablename DESC` + postgresMonitorQueryComment()
	var unusedIndexes []*UnusedIndex
	rows, err := postgresClient.client.Query(unusedIndexesQuery)
	if err != nil {
		return []*UnusedIndex{}
	}
	defer rows.Close()

	for rows.Next() {
		var unusedIndex UnusedIndex
		err := rows.Scan(
			&unusedIndex.Name,
			&unusedIndex.Schema,
			&unusedIndex.TableName,
		)
		if err != nil {
			logger.Error("Index error", "err", err)
			errors.Report(err)
			continue
		}

		unusedIndexes = append(unusedIndexes, &unusedIndex)
	}

	if err := rows.Err(); err != nil {
		logger.Error("Unused Indexes error", "err", err)
		errors.Report(err)
	}

	return unusedIndexes
}

type BloatResult struct {
	Type       string // table or index
	Schemaname string
	Name       string
	Bloat      float64
	Waste      int64
}

// modified from https://github.com/heroku/heroku-pg-extras/blob/main/commands/bloat.js
func (m *SchemaMonitor) FindBloat(postgresClient *PostgresClient) []*BloatResult {
	query := `WITH constants AS (
							SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 4 AS ma
						), bloat_info AS (
							SELECT
								ma,bs,schemaname,tablename,
								(datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
								(maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
							FROM (
								SELECT
									schemaname, tablename, hdr, ma, bs,
									SUM((1-null_frac)*avg_width) AS datawidth,
									MAX(null_frac) AS maxfracsum,
									hdr+(
										SELECT 1+count(*)/8
										FROM pg_stats s2
										WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
									) AS nullhdr
								FROM pg_stats s, constants
								GROUP BY 1,2,3,4,5
							) AS foo
						), table_bloat AS (
							SELECT
								schemaname, tablename, cc.relpages, bs,
								CEIL((cc.reltuples*((datahdr+ma-
									(CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta
							FROM bloat_info
							JOIN pg_class cc ON cc.relname = bloat_info.tablename
							JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
						), index_bloat AS (
							SELECT
								schemaname, tablename, bs,
								COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
								COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
							FROM bloat_info
							JOIN pg_class cc ON cc.relname = bloat_info.tablename
							JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
							JOIN pg_index i ON indrelid = cc.oid
							JOIN pg_class c2 ON c2.oid = i.indexrelid
						)
						SELECT
							type, schemaname, name, bloat, raw_waste as waste
						FROM
						(SELECT
							'table' as type,
							schemaname,
							tablename as name,
							ROUND(CASE WHEN otta=0 THEN 0.0 ELSE table_bloat.relpages/otta::numeric END,1) AS bloat,
							CASE WHEN relpages < otta THEN '0' ELSE (bs*(table_bloat.relpages-otta)::bigint)::bigint END AS raw_waste
						FROM
							table_bloat
								UNION
						SELECT
							'index' as type,
							schemaname,
							iname as name,
							ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS bloat,
							CASE WHEN ipages < iotta THEN '0' ELSE (bs*(ipages-iotta))::bigint END AS raw_waste
						FROM
							index_bloat) bloat_summary
						ORDER BY raw_waste DESC, bloat DESC` + postgresMonitorQueryComment()
	var bloatResults []*BloatResult
	rows, err := postgresClient.client.Query(query)
	if err != nil {
		return []*BloatResult{}
	}
	defer rows.Close()

	for rows.Next() {
		var bloat BloatResult
		err := rows.Scan(
			&bloat.Type,
			&bloat.Schemaname,
			&bloat.Name,
			&bloat.Bloat,
			&bloat.Waste,
		)
		if err != nil {
			log.Printf("%+v", err)
			continue
		}
		bloatResults = append(bloatResults, &bloat)
	}
	return bloatResults
}
