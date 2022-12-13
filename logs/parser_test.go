package logs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldHandleLogLineValid(t *testing.T) {
	valid_lines := []string{
		"707 <134>1 2022-03-24T23:59:31+00:00 host app heroku-postgres - source=HEROKU_POSTGRESQL_GREEN addon=postgresql-opaque-12345 sample#current_transaction=11618849366 sample#db_size=147391814791bytes sample#tables=67 sample#active-connections=41 sample#waiting-connections=0 sample#index-cache-hit-rate=0.99625 sample#table-cache-hit-rate=0.99686 sample#load-avg-1m=0.3575 sample#load-avg-5m=0.3275 sample#load-avg-15m=0.2475 sample#read-iops=302.48 sample#write-iops=351.78 sample#tmp-disk-used=542765056 sample#tmp-disk-available=72436027392 sample#memory-total=31328356kB sample#memory-free=242328kB sample#memory-cached=29518708kB sample#memory-postgres=367856kB sample#wal-percentage-used=0.07767112123673295",
		"740 <134>1 2022-03-24T23:58:26+00:00 host app heroku-postgres - source=HEROKU_POSTGRESQL_PURPLE addon=postgresql-contoured-12345 sample#current_transaction=11618856301 sample#db_size=147391888519bytes sample#tables=67 sample#active-connections=25 sample#waiting-connections=0 sample#index-cache-hit-rate=0.9993 sample#table-cache-hit-rate=0.99643 sample#load-avg-1m=0.345 sample#load-avg-5m=0.23 sample#load-avg-15m=0.205 sample#read-iops=0.15929 sample#write-iops=371.27 sample#tmp-disk-used=543600640 sample#tmp-disk-available=72435191808 sample#memory-total=16085844kB sample#memory-free=243400kB sample#memory-cached=14718708kB sample#memory-postgres=187144kB sample#follower-lag-commits=10441 sample#wal-percentage-used=0.0801535074510631",
		"290 <134>1 2022-03-24T23:59:39+00:00 host app heroku-pgbouncer - source=pgbouncer addon=postgresql-contoured-12345 sample#client_active=56 sample#client_waiting=0 sample#server_active=0 sample#server_idle=16 sample#max_wait=0 sample#avg_query=8751 sample#avg_recv=37568 sample#avg_sent=1339005",
		"290 <134>1 2022-03-24T23:59:39+00:00 host app postgres.1127530 - [GREEN] [14-1]  sql_error_code = 00000 LOG:  duration: 2688.600 ms  execute <unnamed>: SELECT \"users\".* FROM \"users\" LIMIT 1",
		"236 <134>1 2022-03-25T00:54:19+00:00 host app postgres.1657871 - [GREEN] [12-1]  sql_error_code = 00000 LOG:  automatic analyze of table \"dciabc12i2cbbe.public.market_stats\" system usage: CPU: user: 0.46 s, system: 0.12 s, elapsed: 38.04 s",
		"276 <132>1 2022-03-25T00:56:21+00:00 host app postgres.1640916 - [GREEN] [16-2]  sql_error_code = 55P03 STATEMENT:  SELECT \"notifications\".* FROM \"notifications\" WHERE \"notifications\".\"id\" = 258324 LIMIT 1 FOR UPDATE NOWAIT /*app:worker.2,job:NotificationJob*/",
		"276 <132>1 2022-03-25T00:56:21+00:00 host app postgres.1640916 - [GREEN] [14-1]  sql_error_code = 00000 time_ms = \"2022-09-03 15:21:36.137 UTC\" pid=\"248773\" proc_start_time=\"2022-09-03 15:13:32 UTC\" session_id=\"63136f1c.3cbc5\" vtid=\"27/0\" tid=\"0\" log_line=\"1\" database=\"dci0abcdi2c1ee\" connection_source=\"[local]\" user=\"postgres\" application_name=\"Heroku Postgres Backups\" LOG:  duration: 483892.627 ms  statement: SELECT json_build_object('at', 'pg_start_backup', 'lsn', pg_start_backup('681f0f8f_d15c_45b7_bc18_bb17a55f81dc', false, false));",
		"343 <134>1 2022-03-25T00:56:43+00:00 host app postgres.2979 - [PURPLE] [13993-1]  sql_error_code = 00000 LOG:  restartpoint complete: wrote 123520 buffers (32.4%); 0 WAL file(s) added, 15 removed, 25 recycled; write=419.700 s, sync=0.016 s, total=419.834 s; sync files=301, longest=0.003 s, average=0.001 s; distance=689158 kB, estimate=804775 kB",
		"147 <134>1 2022-03-25T00:56:43+00:00 host app postgres.2979 - [PURPLE] [13994-1]  sql_error_code = 00000 LOG:  recovery restart point at 32A8/A515CE48",
		"183 <134>1 2022-03-25T00:56:43+00:00 host app postgres.2979 - [PURPLE] [13994-2]  sql_error_code = 00000 DETAIL:  last completed transaction was at log time 2022-03-25 00:56:42.690423+00",
		"135 <134>1 2022-03-25T00:59:49+00:00 host app postgres.2979 - [PURPLE] [13995-1]  sql_error_code = 00000 LOG:  restartpoint starting: xlog",
	}

	for _, line := range valid_lines {
		if !shouldHandleLogLine(line) {
			t.Error(line)
		}
	}
}

func TestShouldHandleLogLineInvalid(t *testing.T) {
	invalid_lines := []string{
		"",
		"707 <134>1 2022-03-24T23:59:31+00:00 host app worker.1 INFO -- : JOB: job=DataJob since=2022-03-23 18:01:00",
	}

	for _, line := range invalid_lines {
		if shouldHandleLogLine(line) {
			t.Error(line)
		}
	}
}

func TestIsAppPostgresLogLine(t *testing.T) {
	assert.False(t, isAppPostgresStringLogLine(""))
	assert.True(t, isAppPostgresStringLogLine("290 <134>1 2022-03-24T23:59:39+00:00 host app postgres.1127530 - [GREEN] [14-1]  sql_error_code = 00000 LOG:  duration: 2688.600 ms  execute <unnamed>: SELECT \"users\".* FROM \"users\" LIMIT 1"))
}

func TestIsHerokuPostgresLogLine(t *testing.T) {
	assert.False(t, isHerokuPostgresStringLogLine(""))
	assert.True(t, isHerokuPostgresStringLogLine("707 <134>1 2022-03-24T23:59:31+00:00 host app heroku-postgres - source=HEROKU_POSTGRESQL_GREEN addon=postgresql-opaque-12345 sample#current_transaction=11618849366 sample#db_size=147391814791bytes sample#tables=67 sample#active-connections=41 sample#waiting-connections=0 sample#index-cache-hit-rate=0.99625 sample#table-cache-hit-rate=0.99686 sample#load-avg-1m=0.3575 sample#load-avg-5m=0.3275 sample#load-avg-15m=0.2475 sample#read-iops=302.48 sample#write-iops=351.78 sample#tmp-disk-used=542765056 sample#tmp-disk-available=72436027392 sample#memory-total=31328356kB sample#memory-free=242328kB sample#memory-cached=29518708kB sample#memory-postgres=367856kB sample#wal-percentage-used=0.07767112123673295"))
}

func TestIsHerokuPGBouncerLogLine(t *testing.T) {
	assert.False(t, isHerokuPGBouncerStringLogLine(""))
	assert.True(t, isHerokuPGBouncerStringLogLine("290 <134>1 2022-03-24T23:59:39+00:00 host app heroku-pgbouncer - source=pgbouncer addon=postgresql-contoured-12345 sample#client_active=56 sample#client_waiting=0 sample#server_active=0 sample#server_idle=16 sample#max_wait=0 sample#avg_query=8751 sample#avg_recv=37568 sample#avg_sent=1339005"))
}

func TestParseTimestamp(t *testing.T) {
	assert.Equal(t, int64(1648166371), parseTimestamp("2022-03-24T23:59:31+00:00"))
}

func BenchmarkShouldHandleLogLine(b *testing.B) {
	line := "707 <134>1 2022-03-24T23:59:31+00:00 host app heroku-postgres - source=HEROKU_POSTGRESQL_GREEN addon=postgresql-opaque-12345 sample#current_transaction=11618849366 sample#db_size=147391814791bytes sample#tables=67 sample#active-connections=41 sample#waiting-connections=0 sample#index-cache-hit-rate=0.99625 sample#table-cache-hit-rate=0.99686 sample#load-avg-1m=0.3575 sample#load-avg-5m=0.3275 sample#load-avg-15m=0.2475 sample#read-iops=302.48 sample#write-iops=351.78 sample#tmp-disk-used=542765056 sample#tmp-disk-available=72436027392 sample#memory-total=31328356kB sample#memory-free=242328kB sample#memory-cached=29518708kB sample#memory-postgres=367856kB sample#wal-percentage-used=0.07767112123673295"
	for i := 0; i < b.N; i++ {
		shouldHandleLogLine(line)
	}
}
