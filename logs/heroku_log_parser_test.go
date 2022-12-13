package logs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseHerokuPostgresLogLine(t *testing.T) {
	expected := []*ParsedLogLine{
		{
			Metrics: map[string]string{
				"active-connections":   "41",
				"addon":                "postgresql-opaque-12345",
				"current_transaction":  "11618849366",
				"db_size":              "147391814791bytes",
				"index-cache-hit-rate": "0.99625",
				"load-avg-15m":         "0.2475",
				"load-avg-1m":          "0.3575",
				"load-avg-5m":          "0.3275",
				"memory-cached":        "29518708kB",
				"memory-free":          "242328kB",
				"memory-postgres":      "367856kB",
				"memory-total":         "31328356kB",
				"read-iops":            "302.48",
				"source":               "HEROKU_POSTGRESQL_GREEN",
				"table-cache-hit-rate": "0.99686",
				"tables":               "67",
				"timestamp":            "1648166371",
				"tmp-disk-available":   "72436027392",
				"tmp-disk-used":        "542765056",
				"waiting-connections":  "0",
				"wal-percentage-used":  "0.07767112123673295",
				"write-iops":           "351.78",
			},
		},
	}

	assert.Nil(t, parseLogLine(""))
	assert.Equal(t, expected, parseLogLine("707 <134>1 2022-03-24T23:59:31+00:00 host app heroku-postgres - source=HEROKU_POSTGRESQL_GREEN addon=postgresql-opaque-12345 sample#current_transaction=11618849366 sample#db_size=147391814791bytes sample#tables=67 sample#active-connections=41 sample#waiting-connections=0 sample#index-cache-hit-rate=0.99625 sample#table-cache-hit-rate=0.99686 sample#load-avg-1m=0.3575 sample#load-avg-5m=0.3275 sample#load-avg-15m=0.2475 sample#read-iops=302.48 sample#write-iops=351.78 sample#tmp-disk-used=542765056 sample#tmp-disk-available=72436027392 sample#memory-total=31328356kB sample#memory-free=242328kB sample#memory-cached=29518708kB sample#memory-postgres=367856kB sample#wal-percentage-used=0.07767112123673295"))
}

func TestParseHerokuPostgresLogLinePartial(t *testing.T) {
	expected := []*ParsedLogLine{
		{
			Metrics: map[string]string{
				"active-connections":   "41",
				"addon":                "postgresql-opaque-12345",
				"current_transaction":  "11618849366",
				"db_size":              "147391814791bytes",
				"index-cache-hit-rate": "0.99625",
				"load-avg-15m":         "0.2475",
				"load-avg-1m":          "0.3575",
				"load-avg-5m":          "0.3275",
				"memory-cached":        "29518708kB",
				"memory-free":          "242328kB",
				"memory-postgres":      "367856kB",
				"memory-total":         "31328356kB",
				"read-iops":            "302.48",
				"source":               "HEROKU_POSTGRESQL_GREEN",
				"timestamp":            "1648166371",
				"table-cache-hit-rate": "0.99686",
				"tmp-disk-available":   "72436027392",
				"tmp-disk-used":        "542765056",
				"waiting-connections":  "0",
				"wal-percentage-used":  "0.07767112123673295",
				"write-iops":           "351.78",
			},
		},
	}

	// missing tables since partial key=value pair
	assert.Equal(t, expected, parseLogLine("707 <134>1 2022-03-24T23:59:31+00:00 host app heroku-postgres - source=HEROKU_POSTGRESQL_GREEN addon=postgresql-opaque-12345 sample#current_transaction=11618849366 sample#db_size=147391814791bytes sample#tables sample#active-connections=41 sample#waiting-connections=0 sample#index-cache-hit-rate=0.99625 sample#table-cache-hit-rate=0.99686 sample#load-avg-1m=0.3575 sample#load-avg-5m=0.3275 sample#load-avg-15m=0.2475 sample#read-iops=302.48 sample#write-iops=351.78 sample#tmp-disk-used=542765056 sample#tmp-disk-available=72436027392 sample#memory-total=31328356kB sample#memory-free=242328kB sample#memory-cached=29518708kB sample#memory-postgres=367856kB sample#wal-percentage-used=0.07767112123673295"))
}

// pgbouncer log lines are ignored for now because they are flaky
// and are sometimes not even logged
// func TestParseHerokuPGBouncerLogLine(t *testing.T) {
// 	expected := []*ParsedLogLine{
// 		{
// 			Metrics: map[string]string{
// 				"addon":          "postgresql-contoured-12345",
// 				"avg_query":      "8751",
// 				"avg_recv":       "37568",
// 				"avg_sent":       "1339005",
// 				"client_active":  "56",
// 				"client_waiting": "0",
// 				"max_wait":       "0",
// 				"server_active":  "0",
// 				"server_idle":    "16",
// 				"source":         "pgbouncer",
// 				"timestamp":      "1648166379",
// 			},
// 		},
// 	}

// 	assert.Nil(t, parseLogLine(""))
// 	assert.Equal(t, expected, parseLogLine("290 <134>1 2022-03-24T23:59:39+00:00 host app heroku-pgbouncer - source=pgbouncer addon=postgresql-contoured-12345 sample#client_active=56 sample#client_waiting=0 sample#server_active=0 sample#server_idle=16 sample#max_wait=0 sample#avg_query=8751 sample#avg_recv=37568 sample#avg_sent=1339005"))
// }
