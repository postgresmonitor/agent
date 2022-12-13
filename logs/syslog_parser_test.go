package logs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSyslogLineInvalid(t *testing.T) {
	syslogLine := parseSyslogLine("707 <134>1 bar 2022-03-24T23:59:31+00:00 host app")

	assert.Nil(t, syslogLine)
}

func TestParseSyslogLineHerokuPostgres(t *testing.T) {
	syslogLine := parseSyslogLine("707 <134>1 2022-03-24T23:59:31+00:00 host app heroku-postgres - source=HEROKU_POSTGRESQL_GREEN addon=postgresql-opaque-12345 sample#current_transaction=11618849366 sample#db_size=147391814791bytes sample#tables=67 sample#active-connections=41 sample#waiting-connections=0 sample#index-cache-hit-rate=0.99625 sample#table-cache-hit-rate=0.99686 sample#load-avg-1m=0.3575 sample#load-avg-5m=0.3275 sample#load-avg-15m=0.2475 sample#read-iops=302.48 sample#write-iops=351.78 sample#tmp-disk-used=542765056 sample#tmp-disk-available=72436027392 sample#memory-total=31328356kB sample#memory-free=242328kB sample#memory-cached=29518708kB sample#memory-postgres=367856kB sample#wal-percentage-used=0.07767112123673295")

	expected := []*SyslogLine{
		{
			message:   "source=HEROKU_POSTGRESQL_GREEN addon=postgresql-opaque-12345 sample#current_transaction=11618849366 sample#db_size=147391814791bytes sample#tables=67 sample#active-connections=41 sample#waiting-connections=0 sample#index-cache-hit-rate=0.99625 sample#table-cache-hit-rate=0.99686 sample#load-avg-1m=0.3575 sample#load-avg-5m=0.3275 sample#load-avg-15m=0.2475 sample#read-iops=302.48 sample#write-iops=351.78 sample#tmp-disk-used=542765056 sample#tmp-disk-available=72436027392 sample#memory-total=31328356kB sample#memory-free=242328kB sample#memory-cached=29518708kB sample#memory-postgres=367856kB sample#wal-percentage-used=0.07767112123673295",
			process:   "heroku-postgres",
			segment:   "",
			timestamp: "2022-03-24T23:59:31+00:00",
		},
	}
	assert.Equal(t, expected, syslogLine)
}

func TestParseSyslogLineAppPostgres(t *testing.T) {
	syslogLine := parseSyslogLine("343 <134>1 2022-03-25T00:56:43+00:00 host app postgres.2979 - [PURPLE] [13993-1]  sql_error_code = 00000 LOG:  restartpoint complete: wrote 123520 buffers (32.4%); 0 WAL file(s) added, 15 removed, 25 recycled; write=419.700 s, sync=0.016 s, total=419.834 s; sync files=301, longest=0.003 s, average=0.001 s; distance=689158 kB, estimate=804775 kB")

	expected := []*SyslogLine{
		{
			color:     "PURPLE",
			message:   " sql_error_code = 00000 LOG:  restartpoint complete: wrote 123520 buffers (32.4%); 0 WAL file(s) added, 15 removed, 25 recycled; write=419.700 s, sync=0.016 s, total=419.834 s; sync files=301, longest=0.003 s, average=0.001 s; distance=689158 kB, estimate=804775 kB",
			process:   "postgres.2979",
			segment:   "13993",
			timestamp: "2022-03-25T00:56:43+00:00",
		},
	}
	assert.Equal(t, expected, syslogLine)
}

func TestParseSyslogLineTempDiskMultiLine(t *testing.T) {
	syslogLines := parseSyslogLine("518 <134>1 2022-11-15T21:29:04+00:00 host app postgres.1092206 - [DATABASE] [11-1]  sql_error_code = 00000 time_ms = \"2022-11-15 21:29:04.182 UTC\" pid=\"238760\" proc_start_time=\"2022-11-15 21:28:51 UTC\" session_id=\"63740493.3a4a8\" vtid=\"4/191672\" tid=\"0\" log_line=\"4\" database=\"dci5abc2i23dd1\" connection_source=\"123.456.789.012(53548)\" user=\"u9g7i9abcdsum\" application_name=\"postgres-monitor-agent\" LOG:  duration: 12374.865 ms  execute <unnamed>: select queryid, query, calls, total_exec_time, min_exec_time, max_exec_time,\n185 <134>1 2022-11-15T21:29:04+00:00 host app postgres.1092206 - [DATABASE] [11-2] \t\t\t\t\t\t\trows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit,\n178 <134>1 2022-11-15T21:29:04+00:00 host app postgres.1092206 - [DATABASE] [11-3] \t\t\t\t\t\t\tlocal_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,\n116 <134>1 2022-11-15T21:29:04+00:00 host app postgres.1092206 - [DATABASE] [11-4] \t\t\t\t\t\t\tblk_read_time, blk_write_time\n115 <134>1 2022-11-15T21:29:04+00:00 host app postgres.1092206 - [DATABASE] [11-5] \t\t\t\t\t\t\tfrom pg_stat_statements stat\n130 <134>1 2022-11-15T21:29:04+00:00 host app postgres.1092206 - [DATABASE] [11-6] \t\t\t\t\t\t\tjoin pg_database pdb on pdb.oid = stat.dbid\n158 <134>1 2022-11-15T21:29:04+00:00 host app postgres.1092206 - [DATABASE] [11-7] \t\t\t\t\t\t\twhere pdb.datname = current_database() /* app:postgres-monitor-agent */\n496 <134>1 2022-11-15T21:29:04+00:00 host app postgres.1092206 - [DATABASE] [12-1]  sql_error_code = 00000 time_ms = \"2022-11-15 21:29:04.189 UTC\" pid=\"238760\" proc_start_time=\"2022-11-15 21:28:51 UTC\" session_id=\"63740493.3a4a8\" vtid=\"4/191672\" tid=\"0\" log_line=\"5\" database=\"dci5abc2i23dd1\" connection_source=\"123.456.789.0(53548)\" user=\"u9g7i9abcdsum\" application_name=\"postgres-monitor-agent\" LOG:  temporary file: path \"pg_tblspc/16386/PG_14_202107181/pgsql_tmp/pgsql_tmp238760.0\", size 38810127\n")

	expected := []*SyslogLine{
		{
			color:     "DATABASE",
			message:   " sql_error_code = 00000 time_ms = \"2022-11-15 21:29:04.182 UTC\" pid=\"238760\" proc_start_time=\"2022-11-15 21:28:51 UTC\" session_id=\"63740493.3a4a8\" vtid=\"4/191672\" tid=\"0\" log_line=\"4\" database=\"dci5abc2i23dd1\" connection_source=\"123.456.789.012(53548)\" user=\"u9g7i9abcdsum\" application_name=\"postgres-monitor-agent\" LOG:  duration: 12374.865 ms  execute <unnamed>: select queryid, query, calls, total_exec_time, min_exec_time, max_exec_time,\t\t\t\t\t\t\trows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit,\t\t\t\t\t\t\tlocal_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,\t\t\t\t\t\t\tblk_read_time, blk_write_time\t\t\t\t\t\t\tfrom pg_stat_statements stat\t\t\t\t\t\t\tjoin pg_database pdb on pdb.oid = stat.dbid\t\t\t\t\t\t\twhere pdb.datname = current_database() /* app:postgres-monitor-agent */",
			process:   "postgres.1092206",
			segment:   "11",
			timestamp: "2022-11-15T21:29:04+00:00",
		},
		{
			color:     "DATABASE",
			message:   " sql_error_code = 00000 time_ms = \"2022-11-15 21:29:04.189 UTC\" pid=\"238760\" proc_start_time=\"2022-11-15 21:28:51 UTC\" session_id=\"63740493.3a4a8\" vtid=\"4/191672\" tid=\"0\" log_line=\"5\" database=\"dci5abc2i23dd1\" connection_source=\"123.456.789.0(53548)\" user=\"u9g7i9abcdsum\" application_name=\"postgres-monitor-agent\" LOG:  temporary file: path \"pg_tblspc/16386/PG_14_202107181/pgsql_tmp/pgsql_tmp238760.0\", size 38810127",
			process:   "postgres.1092206",
			segment:   "12",
			timestamp: "2022-11-15T21:29:04+00:00",
		},
	}
	assert.Equal(t, expected, syslogLines)
}

func TestParseSyslogLineSlowQueryMultiLine(t *testing.T) {
	syslogLine := parseSyslogLine("976 <134>1 2022-08-11T03:22:11+00:00 host app postgres.3811305 - [GREEN] [12-1]  sql_error_code = 00000 time_ms = \"2022-08-11 03:22:11.987 UTC\" pid=\"278908\" proc_start_time=\"2022-08-11 03:18:10 UTC\" session_id=\"62f474f2.4417c\" vtid=\"34/62754598\" tid=\"0\" log_line=\"4\" database=\"dci5abc2i23dd1\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 5.2.10 app [6 of 16 busy] - 123.456.789.1:28429\" LOG:  duration: 2681.599 ms  execute <unnamed>: SELECT \"users\".* FROM \"users\" INNER JOIN \"accounts\" ON \"accounts\".\"id\" = \"users\".\"account_id\" INNER JOIN \"subscriptions\" ON \"subscriptions\".\"account_id\" = \"accounts\".\"id\" WHERE (subscriptions.ended_at >\n412 <134>1 2022-08-11T03:22:11+00:00 host app postgres.3811305 - [GREEN] [12-2]  '2022-08-11 03:22:09.289039') AND \"plans\".\"name\" = 'Free' GROUP BY \"users\".\"id\" HAVING (COUNT(notifications.id) >= 100) ORDER BY \"users\".\"id\" ASC LIMIT 1000 /*app:data-worker,job:RateLimitWorker*/\n")

	expected := []*SyslogLine{
		{
			color:     "GREEN",
			message:   " sql_error_code = 00000 time_ms = \"2022-08-11 03:22:11.987 UTC\" pid=\"278908\" proc_start_time=\"2022-08-11 03:18:10 UTC\" session_id=\"62f474f2.4417c\" vtid=\"34/62754598\" tid=\"0\" log_line=\"4\" database=\"dci5abc2i23dd1\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 5.2.10 app [6 of 16 busy] - 123.456.789.1:28429\" LOG:  duration: 2681.599 ms  execute <unnamed>: SELECT \"users\".* FROM \"users\" INNER JOIN \"accounts\" ON \"accounts\".\"id\" = \"users\".\"account_id\" INNER JOIN \"subscriptions\" ON \"subscriptions\".\"account_id\" = \"accounts\".\"id\" WHERE (subscriptions.ended_at > '2022-08-11 03:22:09.289039') AND \"plans\".\"name\" = 'Free' GROUP BY \"users\".\"id\" HAVING (COUNT(notifications.id) >= 100) ORDER BY \"users\".\"id\" ASC LIMIT 1000 /*app:data-worker,job:RateLimitWorker*/",
			process:   "postgres.3811305",
			segment:   "12",
			timestamp: "2022-08-11T03:22:11+00:00",
		},
	}
	assert.Equal(t, expected, syslogLine)
}

func TestParseSyslogLineUniqueIndexErrorMultiLine(t *testing.T) {
	syslogLine := parseSyslogLine("531 <132>1 2022-08-11T02:47:50+00:00 host app postgres.3796759 - [GREEN] [11-1]  sql_error_code = 23505 time_ms = \"2022-08-11 02:47:50.521 UTC\" pid=\"275326\" proc_start_time=\"2022-08-11 02:26:10 UTC\" session_id=\"62f468c2.4337e\" vtid=\"8/75647105\" tid=\"2423517980\" log_line=\"1\" database=\"dciabc12i2cbbe\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 5.2.10 app [6 of 16 busy] - 123.456.789.1:28429\" ERROR:  duplicate key value violates unique constraint \"idx_index_123\"\n523 <132>1 2022-08-11T02:47:50+00:00 host app postgres.3796759 - [GREEN] [11-2]  sql_error_code = 23505 time_ms = \"2022-08-11 02:47:50.521 UTC\" pid=\"275326\" proc_start_time=\"2022-08-11 02:26:10 UTC\" session_id=\"62f468c2.4337e\" vtid=\"8/75647105\" tid=\"2423517980\" log_line=\"2\" database=\"dci54c12i23dd1\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 6.2.10 app [6 of 16 busy] - 123.456.789.1:28429\" DETAIL:  Key (user_id, alert_id, notification_id)=(345769, 68003128, 717838) already exists.\n749 <132>1 2022-08-11T02:47:50+00:00 host app postgres.3796759 - [GREEN] [11-3]  sql_error_code = 23505 time_ms = \"2022-08-11 02:47:50.521 UTC\" pid=\"275326\" proc_start_time=\"2022-08-11 02:26:10 UTC\" session_id=\"62f468c2.4337e\" vtid=\"8/75647105\" tid=\"2423517980\" log_line=\"3\" database=\"dciabc12i2cbbe\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 5.2.10 app [6 of 16 busy] - 123.456.789.1:28429\" STATEMENT:  INSERT INTO \"notifications\" (\"user_id\", \"notification_channel_id\", \"alert_id\", \"created_at\", \"updated_at\", \"send_at\") VALUES (345769, 717838, 68003128, '2022-08-11 02:47:50.360767', '2022-08-11 02:47:50.360767', '2022-08-11 02:47:50.359679') RETURNING \"id\" /*app:alert-worker-wfhq,job:NotificationsWorker*/\n")

	expected := []*SyslogLine{
		{
			color:     "GREEN",
			message:   " sql_error_code = 23505 time_ms = \"2022-08-11 02:47:50.521 UTC\" pid=\"275326\" proc_start_time=\"2022-08-11 02:26:10 UTC\" session_id=\"62f468c2.4337e\" vtid=\"8/75647105\" tid=\"2423517980\" log_line=\"1\" database=\"dciabc12i2cbbe\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 5.2.10 app [6 of 16 busy] - 123.456.789.1:28429\" ERROR:  duplicate key value violates unique constraint \"idx_index_123\" sql_error_code = 23505 time_ms = \"2022-08-11 02:47:50.521 UTC\" pid=\"275326\" proc_start_time=\"2022-08-11 02:26:10 UTC\" session_id=\"62f468c2.4337e\" vtid=\"8/75647105\" tid=\"2423517980\" log_line=\"2\" database=\"dci54c12i23dd1\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 6.2.10 app [6 of 16 busy] - 123.456.789.1:28429\" DETAIL:  Key (user_id, alert_id, notification_id)=(345769, 68003128, 717838) already exists. sql_error_code = 23505 time_ms = \"2022-08-11 02:47:50.521 UTC\" pid=\"275326\" proc_start_time=\"2022-08-11 02:26:10 UTC\" session_id=\"62f468c2.4337e\" vtid=\"8/75647105\" tid=\"2423517980\" log_line=\"3\" database=\"dciabc12i2cbbe\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 5.2.10 app [6 of 16 busy] - 123.456.789.1:28429\" STATEMENT:  INSERT INTO \"notifications\" (\"user_id\", \"notification_channel_id\", \"alert_id\", \"created_at\", \"updated_at\", \"send_at\") VALUES (345769, 717838, 68003128, '2022-08-11 02:47:50.360767', '2022-08-11 02:47:50.360767', '2022-08-11 02:47:50.359679') RETURNING \"id\" /*app:alert-worker-wfhq,job:NotificationsWorker*/",
			process:   "postgres.3796759",
			segment:   "11",
			timestamp: "2022-08-11T02:47:50+00:00",
		},
	}
	assert.Equal(t, expected, syslogLine)
}

func TestParseSyslogLineRelatedMultiLine(t *testing.T) {

	syslogLine := parseSyslogLine("531 <132>1 2022-11-11T23:01:22.000000+00:00 host app postgres.165610 - [DATABASE] [307-1]  sql_error_code = 00000 time_ms = \"2022-11-11 23:01:22.844 UTC\" pid=\"22221\" proc_start_time=\"2022-11-11 19:18:11 UTC\" session_id=\"636e9ff3.56cd\" vtid=\"34/3671\" tid=\"0\" log_line=\"364\" database=\"dciabc12i2cbbe\" connection_source=\"123.456.789.012(59342)\" user=\"uestabchn12ae1\" application_name=\"app\" LOG:  duration: 10988.135 ms  execute <unnamed>: select *\n531 <132>1 2022-11-11T23:01:22.000000+00:00 host app postgres.165610 - [DATABASE] [307-2]  from foo /* comment */")

	expected := []*SyslogLine{
		{
			color:     "DATABASE",
			message:   " sql_error_code = 00000 time_ms = \"2022-11-11 23:01:22.844 UTC\" pid=\"22221\" proc_start_time=\"2022-11-11 19:18:11 UTC\" session_id=\"636e9ff3.56cd\" vtid=\"34/3671\" tid=\"0\" log_line=\"364\" database=\"dciabc12i2cbbe\" connection_source=\"123.456.789.012(59342)\" user=\"uestabchn12ae1\" application_name=\"app\" LOG:  duration: 10988.135 ms  execute <unnamed>: select * from foo /* comment */",
			process:   "postgres.165610",
			segment:   "307",
			timestamp: "2022-11-11T23:01:22.000000+00:00",
		},
	}
	assert.Equal(t, expected, syslogLine)
}

func TestParseSyslogLineUnrelatedMultiLine(t *testing.T) {
	syslogLine := parseSyslogLine("531 <132>1 2022-08-11T02:47:50+00:00 host app postgres.3796759 - [GREEN] [12-1]  sql_error_code = 00000 time_ms = \"2022-10-22 04:40:14.014 UTC\" pid=\"2769791\" proc_start_time=\"2022-10-22 04:26:12 UTC\" session_id=\"635370e4.2a437f\" vtid=\"33/76444444\" tid=\"4122963357\" log_line=\"3\" database=\"dciabc12i2cbbe\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 5.2.10 app [4 of 16 busy] - 123.456.789.1:28429\" LOG:  duration: 30510.285 ms  execute <unnamed>: DELETE FROM \"users\" WHERE (created < '2022-10-19 04:39:43.497312') /*app:worker,job:DeleteUsersJob*/\n531 <132>1 2022-08-11T02:47:50+00:00 host app postgres.69018 - [GREEN] [12453-1]  sql_error_code = 00000 time_ms = \"2022-10-22 04:40:14.217 UTC\" pid=\"8\" proc_start_time=\"2022-09-10 02:35:17 UTC\" session_id=\"631abce5.8\" vtid=\"\" tid=\"0\" log_line=\"12448\" LOG:  checkpoint complete: wrote 74816 buffers (9.8%); 0 WAL file(s) added, 1 removed, 52 recycled; write=190.504 s, sync=0.056 s, total=190.697 s; sync files=15, longest=0.014 s, average=0.001 s; distance=860461 kB, estimate=907017 kB")

	expected := []*SyslogLine{
		{
			color:     "GREEN",
			message:   " sql_error_code = 00000 time_ms = \"2022-10-22 04:40:14.014 UTC\" pid=\"2769791\" proc_start_time=\"2022-10-22 04:26:12 UTC\" session_id=\"635370e4.2a437f\" vtid=\"33/76444444\" tid=\"4122963357\" log_line=\"3\" database=\"dciabc12i2cbbe\" connection_source=\"[local]\" user=\"uestabchn12ae1\" application_name=\"sidekiq 5.2.10 app [4 of 16 busy] - 123.456.789.1:28429\" LOG:  duration: 30510.285 ms  execute <unnamed>: DELETE FROM \"users\" WHERE (created < '2022-10-19 04:39:43.497312') /*app:worker,job:DeleteUsersJob*/",
			process:   "postgres.3796759",
			segment:   "12",
			timestamp: "2022-08-11T02:47:50+00:00",
		},
		{
			color:     "GREEN",
			message:   " sql_error_code = 00000 time_ms = \"2022-10-22 04:40:14.217 UTC\" pid=\"8\" proc_start_time=\"2022-09-10 02:35:17 UTC\" session_id=\"631abce5.8\" vtid=\"\" tid=\"0\" log_line=\"12448\" LOG:  checkpoint complete: wrote 74816 buffers (9.8%); 0 WAL file(s) added, 1 removed, 52 recycled; write=190.504 s, sync=0.056 s, total=190.697 s; sync files=15, longest=0.014 s, average=0.001 s; distance=860461 kB, estimate=907017 kB",
			process:   "postgres.69018",
			segment:   "12453",
			timestamp: "2022-08-11T02:47:50+00:00",
		},
	}
	assert.Equal(t, expected, syslogLine)
}

func TestParseSyslogLinePGBouncer(t *testing.T) {
	syslogLine := parseSyslogLine("290 <134>1 2022-03-24T23:59:39+00:00 host app heroku-pgbouncer - source=pgbouncer addon=postgresql-contoured-12345 sample#client_active=56 sample#client_waiting=0 sample#server_active=0 sample#server_idle=16 sample#max_wait=0 sample#avg_query=8751 sample#avg_recv=37568 sample#avg_sent=1339005")

	expected := []*SyslogLine{
		{
			message:   "source=pgbouncer addon=postgresql-contoured-12345 sample#client_active=56 sample#client_waiting=0 sample#server_active=0 sample#server_idle=16 sample#max_wait=0 sample#avg_query=8751 sample#avg_recv=37568 sample#avg_sent=1339005",
			process:   "heroku-pgbouncer",
			segment:   "",
			timestamp: "2022-03-24T23:59:39+00:00",
		},
	}
	assert.Equal(t, expected, syslogLine)
}
