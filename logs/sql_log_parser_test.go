package logs

import (
	"agent/db"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSqlSyslogLineValid(t *testing.T) {
	line := &SyslogLine{
		color:     "GREEN",
		message:   " sql_error_code = 00000 time_ms = \"2022-08-11 03:22:11.987 UTC\" pid=\"278908\" proc_start_time=\"2022-08-11 03:18:10 UTC\" session_id=\"62f474f2.4417c\" vtid=\"34/62754118\" tid=\"0\" log_line=\"4\" database=\"dbiabc12i21234\" connection_source=\"[local]\" user=\"ueab123hn12abc\" application_name=\"sidekiq 6.2.10 app [6 of 16 busy] - 1.123.456.789:28429\" LOG:  duration: 2681.599 ms  execute <unnamed>: SELECT \"users\".* FROM \"users\" INNER JOIN \"accounts\" ON \"accounts\".\"id\" = \"users\".\"account_id\" INNER JOIN \"subscriptions\" ON \"subscriptions\".\"account_id\" = \"accounts\".\"id\" WHERE (subscriptions.ended_at > '2022-08-11 03:22:09.289039') AND \"plans\".\"name\" = 'Free' GROUP BY \"users\".\"id\" HAVING (COUNT(alerts.id) >= 100) ORDER BY \"users\".\"id\" ASC LIMIT 1000 /*app:worker,job:AlertWorker*/",
		process:   "postgres.3811305",
		timestamp: "2022-08-11T03:22:11+00:00",
	}

	expected := &ParsedLogLine{
		SlowQuery: &db.SlowQuery{
			SqlErrorCode:     "00000",
			Metadata:         "time_ms = \"2022-08-11 03:22:11.987 UTC\" pid=\"278908\" proc_start_time=\"2022-08-11 03:18:10 UTC\" session_id=\"62f474f2.4417c\" vtid=\"34/62754118\" tid=\"0\" log_line=\"4\" database=\"dbiabc12i21234\" connection_source=\"[local]\" user=\"ueab123hn12abc\" application_name=\"sidekiq 6.2.10 app [6 of 16 busy] - 1.123.456.789:28429\"",
			DurationMs:       2681.599,
			Raw:              "SELECT \"users\".* FROM \"users\" INNER JOIN \"accounts\" ON \"accounts\".\"id\" = \"users\".\"account_id\" INNER JOIN \"subscriptions\" ON \"subscriptions\".\"account_id\" = \"accounts\".\"id\" WHERE (subscriptions.ended_at > '2022-08-11 03:22:09.289039') AND \"plans\".\"name\" = 'Free' GROUP BY \"users\".\"id\" HAVING (COUNT(alerts.id) >= 100) ORDER BY \"users\".\"id\" ASC LIMIT 1000 /*app:worker,job:AlertWorker*/",
			ServerConfigName: "GREEN",
			MeasuredAt:       1660188131,
		},
	}

	parsed := parseSqlSyslogLine(line)

	assert.Equal(t, expected, parsed)
}

func TestParseSqlSyslogLineInvalid(t *testing.T) {
	invalid_messages := []string{
		"sql_error_code = 00000 LOG:  automatic analyze of table \"dbiabc12i21234.public.stats\" system usage: CPU: user: 0.46 s, system: 0.12 s, elapsed: 38.04 s",
		"sql_error_code = 55P03 STATEMENT:  SELECT \"alerts\".* FROM \"alerts\" WHERE \"alerts\".\"id\" = 258324 LIMIT 1 FOR UPDATE NOWAIT /*app:worker.2,job:Job*/",
		"sql_error_code = 00000 LOG:  restartpoint complete: wrote 123520 buffers (32.4%); 0 WAL file(s) added, 15 removed, 25 recycled; write=419.700 s, sync=0.016 s, total=419.834 s; sync files=301, longest=0.003 s, average=0.001 s; distance=689158 kB, estimate=804775 kB",
		"sql_error_code = 00000 LOG:  recovery restart point at 32A8/A515CE48",
		"sql_error_code = 00000 DETAIL:  last completed transaction was at log time 2022-03-25 00:56:42.690423+00",
		"sql_error_code = 00000 LOG:  restartpoint starting: xlog",
	}

	for _, message := range invalid_messages {
		line := &SyslogLine{
			color:     "GREEN",
			message:   message,
			process:   "postgres.3811305",
			timestamp: "2022-08-11T03:22:11+00:00",
		}
		parsed := parseSqlSyslogLine(line)

		if !assert.Nil(t, parsed) {
			t.Error(line)
		}
	}
}
