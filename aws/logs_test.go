package aws

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRDSLogFile(t *testing.T) {
	raw := "2023-10-30 21:01:17 UTC::@:[390]:LOG:  checkpoint starting: time\n2023-10-28 19:01:19 UTC:123.456.789.123(57157):user@database:[13441]:ERROR:  Function pg_stat_get_wal_receiver() is currently not supported in Aurora\n2023-10-30 18:02:15 UTC:123.456.789.123(61810):postgres_monitor_agent@database:[23463]:LOG:  could not receive data from client: Connection reset by peer\n2023-10-30 21:01:17 UTC::@:[390]:LOG:  checkpoint complete: wrote 3 buffers (0.0%); 0 WAL file(s) added, 0 removed, 1 recycled; write=0.203 s, sync=0.003 s, total=0.215 s; sync files=3, longest=0.003 s, average=0.001 s; distance=65536 kB, estimate=65562 kB\n2023-10-30 18:39:48 UTC:123.456.789.123(62577):postgres@database:[3546]:LOG:  duration: 8622.276 ms  plan:\n\tQuery Text: select queryid, query, calls, total_exec_time, min_exec_time, max_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time from pg_stat_statements stat join pg_database pdb on pdb.oid = stat.dbid where pdb.datname = current_database() /* app:postgres-monitor-agent */\n\tHash Join  (cost=1.04..13.73 rows=5 width=176)\n\t  Hash Cond: (pg_stat_statements.dbid = pdb.oid)\n\t  ->  Function Scan on pg_stat_statements  (cost=0.00..10.00 rows=1000 width=180)\n\t  ->  Hash  (cost=1.03..1.03 rows=1 width=4)\n\t        ->  Seq Scan on pg_database pdb  (cost=0.00..1.03 rows=1 width=4)\n\t              Filter: (datname = current_database())\n"

	parsedLogLines := ParseRDSLogFile(raw)

	parsedLine1 := &RDSLogLine{
		Timestamp: 1698699677,
		IP:        net.ParseIP(""),
		Username:  "",
		Database:  "",
		Level:     "LOG",
		Message:   "checkpoint starting: time",
	}
	parsedLine2 := &RDSLogLine{
		Timestamp: 1698519679,
		IP:        net.ParseIP("123.456.789.123"),
		Username:  "user",
		Database:  "database",
		Level:     "ERROR",
		Message:   "Function pg_stat_get_wal_receiver() is currently not supported in Aurora",
	}
	parsedLine3 := &RDSLogLine{
		Timestamp: 1698688935,
		IP:        net.ParseIP("123.456.789.123"),
		Username:  "postgres_monitor_agent",
		Database:  "database",
		Level:     "LOG",
		Message:   "could not receive data from client: Connection reset by peer",
	}
	parsedLine4 := &RDSLogLine{
		Timestamp: 1698699677,
		IP:        net.ParseIP(""),
		Username:  "",
		Database:  "",
		Level:     "LOG",
		Message:   "checkpoint complete: wrote 3 buffers (0.0%); 0 WAL file(s) added, 0 removed, 1 recycled; write=0.203 s, sync=0.003 s, total=0.215 s; sync files=3, longest=0.003 s, average=0.001 s; distance=65536 kB, estimate=65562 kB",
	}
	parsedLine5 := &RDSLogLine{
		Timestamp: 1698691188,
		IP:        net.ParseIP("123.456.789.123"),
		Username:  "postgres",
		Database:  "database",
		Level:     "LOG",
		Message:   "duration: 8622.276 ms  plan:\n\tQuery Text: select queryid, query, calls, total_exec_time, min_exec_time, max_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time from pg_stat_statements stat join pg_database pdb on pdb.oid = stat.dbid where pdb.datname = current_database() /* app:postgres-monitor-agent */\n\tHash Join  (cost=1.04..13.73 rows=5 width=176)\n\t  Hash Cond: (pg_stat_statements.dbid = pdb.oid)\n\t  ->  Function Scan on pg_stat_statements  (cost=0.00..10.00 rows=1000 width=180)\n\t  ->  Hash  (cost=1.03..1.03 rows=1 width=4)\n\t        ->  Seq Scan on pg_database pdb  (cost=0.00..1.03 rows=1 width=4)\n\t              Filter: (datname = current_database())",
	}

	assert.Equal(t, parsedLine1.Timestamp, parsedLogLines[0].Timestamp)
	assert.Equal(t, parsedLine1.IP, parsedLogLines[0].IP)
	assert.Equal(t, parsedLine1.Username, parsedLogLines[0].Username)
	assert.Equal(t, parsedLine1.Database, parsedLogLines[0].Database)
	assert.Equal(t, parsedLine1.Level, parsedLogLines[0].Level)
	assert.Equal(t, parsedLine1.Message, parsedLogLines[0].Message)

	assert.Equal(t, parsedLine2.Timestamp, parsedLogLines[1].Timestamp)
	assert.Equal(t, parsedLine2.IP, parsedLogLines[1].IP)
	assert.Equal(t, parsedLine2.Username, parsedLogLines[1].Username)
	assert.Equal(t, parsedLine2.Database, parsedLogLines[1].Database)
	assert.Equal(t, parsedLine2.Level, parsedLogLines[1].Level)
	assert.Equal(t, parsedLine2.Message, parsedLogLines[1].Message)

	assert.Equal(t, parsedLine3.Timestamp, parsedLogLines[2].Timestamp)
	assert.Equal(t, parsedLine3.IP, parsedLogLines[2].IP)
	assert.Equal(t, parsedLine3.Username, parsedLogLines[2].Username)
	assert.Equal(t, parsedLine3.Database, parsedLogLines[2].Database)
	assert.Equal(t, parsedLine3.Level, parsedLogLines[2].Level)
	assert.Equal(t, parsedLine3.Message, parsedLogLines[2].Message)

	assert.Equal(t, parsedLine4.Timestamp, parsedLogLines[3].Timestamp)
	assert.Equal(t, parsedLine4.IP, parsedLogLines[3].IP)
	assert.Equal(t, parsedLine4.Username, parsedLogLines[3].Username)
	assert.Equal(t, parsedLine4.Database, parsedLogLines[3].Database)
	assert.Equal(t, parsedLine4.Level, parsedLogLines[3].Level)
	assert.Equal(t, parsedLine4.Message, parsedLogLines[3].Message)

	assert.Equal(t, parsedLine5.Timestamp, parsedLogLines[4].Timestamp)
	assert.Equal(t, parsedLine5.IP, parsedLogLines[4].IP)
	assert.Equal(t, parsedLine5.Username, parsedLogLines[4].Username)
	assert.Equal(t, parsedLine5.Database, parsedLogLines[4].Database)
	assert.Equal(t, parsedLine5.Level, parsedLogLines[4].Level)
	assert.Equal(t, parsedLine5.Message, parsedLogLines[4].Message)
}

func TestParseRDSLogLineTimestamp(t *testing.T) {
	assert.Equal(t, int64(-1), ParseRDSLogLineTimestamp(""))
	assert.Equal(t, int64(-1), ParseRDSLogLineTimestamp("18:39:48 UTC"))
	assert.Equal(t, int64(-1), ParseRDSLogLineTimestamp("123.456.789.123(61810)"))

	assert.Equal(t, int64(1698691188), ParseRDSLogLineTimestamp("2023-10-30 18:39:48 UTC"))
}

func TestStitchRDSLogLinesIntoChunks(t *testing.T) {
	logLines := StitchRDSLogLinesIntoChunks("2023-10-30 18:39:48 UTC:123.456.789.123(62577):postgres@database:[3546]:LOG:  duration: 8622.276 ms  plan:\n\tQuery Text: select queryid, query, calls, total_exec_time, min_exec_time, max_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time from pg_stat_statements stat join pg_database pdb on pdb.oid = stat.dbid where pdb.datname = current_database() /* app:postgres-monitor-agent */\n\tHash Join  (cost=1.04..13.73 rows=5 width=176)\n\t  Hash Cond: (pg_stat_statements.dbid = pdb.oid)\n\t  ->  Function Scan on pg_stat_statements  (cost=0.00..10.00 rows=1000 width=180)\n\t  ->  Hash  (cost=1.03..1.03 rows=1 width=4)\n\t        ->  Seq Scan on pg_database pdb  (cost=0.00..1.03 rows=1 width=4)\n\t              Filter: (datname = current_database())\n2023-10-30 18:02:15 UTC:123.456.789.123(61810):postgres_monitor_agent@database:[23463]:LOG:  could not receive data from client: Connection reset by peer\n")
	logLine1 := "2023-10-30 18:39:48 UTC:123.456.789.123(62577):postgres@database:[3546]:LOG:  duration: 8622.276 ms  plan:\n\tQuery Text: select queryid, query, calls, total_exec_time, min_exec_time, max_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time from pg_stat_statements stat join pg_database pdb on pdb.oid = stat.dbid where pdb.datname = current_database() /* app:postgres-monitor-agent */\n\tHash Join  (cost=1.04..13.73 rows=5 width=176)\n\t  Hash Cond: (pg_stat_statements.dbid = pdb.oid)\n\t  ->  Function Scan on pg_stat_statements  (cost=0.00..10.00 rows=1000 width=180)\n\t  ->  Hash  (cost=1.03..1.03 rows=1 width=4)\n\t        ->  Seq Scan on pg_database pdb  (cost=0.00..1.03 rows=1 width=4)\n\t              Filter: (datname = current_database())"
	logLine2 := "2023-10-30 18:02:15 UTC:123.456.789.123(61810):postgres_monitor_agent@database:[23463]:LOG:  could not receive data from client: Connection reset by peer"

	assert.Equal(t, logLine1, logLines[0])
	assert.Equal(t, logLine2, logLines[1])
}

func TestParseRDSQueryExplain(t *testing.T) {
	logLine := &RDSLogLine{
		Timestamp: 1698699677,
		IP:        net.ParseIP("123.456.789.123"),
		Username:  "postgres",
		Database:  "database",
		Level:     "LOG",
		Message:   "duration: 8622.276 ms  plan:\n\tQuery Text: select queryid, query, calls, total_exec_time, min_exec_time, max_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time from pg_stat_statements stat join pg_database pdb on pdb.oid = stat.dbid where pdb.datname = current_database() /* app:postgres-monitor-agent */\n\tHash Join  (cost=1.04..13.73 rows=5 width=176)\n\t  Hash Cond: (pg_stat_statements.dbid = pdb.oid)\n\t  ->  Function Scan on pg_stat_statements  (cost=0.00..10.00 rows=1000 width=180)\n\t  ->  Hash  (cost=1.03..1.03 rows=1 width=4)\n\t        ->  Seq Scan on pg_database pdb  (cost=0.00..1.03 rows=1 width=4)\n\t              Filter: (datname = current_database())",
	}

	slowQuery := ParseRDSQueryExplain(logLine, "rds-instance-id-1")
	assert.NotNil(t, slowQuery)
	assert.Equal(t, 8622.276, slowQuery.DurationMs)
	assert.Equal(t, "select queryid, query, calls, total_exec_time, min_exec_time, max_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time from pg_stat_statements stat join pg_database pdb on pdb.oid = stat.dbid where pdb.datname = current_database() /* app:postgres-monitor-agent */", slowQuery.Raw)
	assert.Equal(t, "Hash Join  (cost=1.04..13.73 rows=5 width=176)\n      Hash Cond: (pg_stat_statements.dbid = pdb.oid)\n      ->  Function Scan on pg_stat_statements  (cost=0.00..10.00 rows=1000 width=180)\n      ->  Hash  (cost=1.03..1.03 rows=1 width=4)\n            ->  Seq Scan on pg_database pdb  (cost=0.00..1.03 rows=1 width=4)\n                  Filter: (datname = current_database())", slowQuery.Explain)
	assert.Equal(t, int64(1698699677), slowQuery.MeasuredAt)
	assert.Equal(t, "rds-instance-id-1", slowQuery.ServerName)

	logLine2 := &RDSLogLine{
		Timestamp: 1698699677,
		IP:        net.ParseIP("123.456.789.123"),
		Username:  "postgres",
		Database:  "database",
		Level:     "LOG",
		Message:   "could not receive data from client: Connection reset by peer",
	}
	assert.Nil(t, ParseRDSQueryExplain(logLine2, "rds-instance-id-1"))
}
