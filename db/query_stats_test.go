package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryStatsDelta(t *testing.T) {
	previous := &QueryStats{
		QueryId:             1855550508,
		Query:               "UPDATE \"stats\" SET \"value\" = ?, \"updated_at\" = ? WHERE \"stats\".\"id\" = ?",
		Comment:             "/*app:worker-1*/",
		Calls:               16184,
		TotalTime:           406.36835800000057,
		MinTime:             0.01231,
		MaxTime:             4.895439,
		MeanTime:            0.025109265818091885,
		Rows:                16184,
		SharedBlocksHit:     104553,
		SharedBlocksRead:    0,
		SharedBlocksDirtied: 15,
		SharedBlocksWritten: 990,
		LocalBlocksHit:      0,
		LocalBlocksRead:     0,
		LocalBlocksDirtied:  0,
		LocalBlocksWritten:  0,
		TempBlocksRead:      0,
		TempBlocksWritten:   0,
		BlockReadTime:       4.948159,
		BlockWriteTime:      0,
	}

	current := &QueryStats{
		QueryId:             1855550508,
		Query:               "UPDATE \"stats\" SET \"value\" = ?, \"updated_at\" = ? WHERE \"stats\".\"id\" = ?",
		Comment:             "/*app:worker-1*/",
		Calls:               16194,
		TotalTime:           416.36835800000057,
		MinTime:             0.01231,
		MaxTime:             4.895439,
		MeanTime:            0.025109265818091885,
		Rows:                16284,
		SharedBlocksHit:     104563,
		SharedBlocksRead:    3,
		SharedBlocksDirtied: 25,
		SharedBlocksWritten: 991,
		LocalBlocksHit:      0,
		LocalBlocksRead:     0,
		LocalBlocksDirtied:  0,
		LocalBlocksWritten:  0,
		TempBlocksRead:      0,
		TempBlocksWritten:   0,
		BlockReadTime:       5.948159,
		BlockWriteTime:      1,
	}

	delta := previous.Delta(current)

	assert.Equal(t, int64(1855550508), delta.QueryId)
	assert.Equal(t, "UPDATE \"stats\" SET \"value\" = ?, \"updated_at\" = ? WHERE \"stats\".\"id\" = ?", delta.Query)
	assert.Equal(t, "/*app:worker-1*/", delta.Comment)
	assert.Equal(t, int64(10), delta.Calls)
	assert.Equal(t, 10.0, delta.TotalTime)
	assert.Equal(t, 0.01231, delta.MinTime)
	assert.Equal(t, 4.895439, delta.MaxTime)
	assert.Equal(t, 1.0, delta.MeanTime)
	assert.Equal(t, int64(100), delta.Rows)
	assert.Equal(t, int64(10), delta.SharedBlocksHit)
	assert.Equal(t, int64(3), delta.SharedBlocksRead)
	assert.Equal(t, int64(10), delta.SharedBlocksDirtied)
	assert.Equal(t, int64(1), delta.SharedBlocksWritten)
	assert.Equal(t, int64(0), delta.LocalBlocksHit)
	assert.Equal(t, int64(0), delta.LocalBlocksRead)
	assert.Equal(t, int64(0), delta.LocalBlocksDirtied)
	assert.Equal(t, int64(0), delta.LocalBlocksWritten)
	assert.Equal(t, int64(0), delta.TempBlocksRead)
	assert.Equal(t, int64(0), delta.TempBlocksWritten)
	assert.Equal(t, 1.0, delta.BlockReadTime)
	assert.Equal(t, 1.0, delta.BlockWriteTime)
}

func TestAggregate(t *testing.T) {
	previous := &QueryStats{
		QueryId:             1855550508,
		Query:               "UPDATE \"stats\" SET \"value\" = ?, \"updated_at\" = ? WHERE \"stats\".\"id\" = ?",
		Comment:             "/*app:worker-1*/",
		Calls:               16184,
		TotalTime:           406.36835800000057,
		MinTime:             0.01231,
		MaxTime:             4.895439,
		MeanTime:            0.025109265818091885,
		Rows:                16184,
		SharedBlocksHit:     104553,
		SharedBlocksRead:    0,
		SharedBlocksDirtied: 15,
		SharedBlocksWritten: 990,
		LocalBlocksHit:      0,
		LocalBlocksRead:     0,
		LocalBlocksDirtied:  0,
		LocalBlocksWritten:  0,
		TempBlocksRead:      0,
		TempBlocksWritten:   0,
		BlockReadTime:       4.948159,
		BlockWriteTime:      0,
	}

	current := &QueryStats{
		QueryId:             1855550508,
		Query:               "UPDATE \"stats\" SET \"value\" = ?, \"updated_at\" = ? WHERE \"stats\".\"id\" = ?",
		Comment:             "/*app:worker-1*/",
		Calls:               16194,
		TotalTime:           506.36835800000057,
		MinTime:             0.001231,
		MaxTime:             5.895439,
		MeanTime:            0.025109265818091885,
		Rows:                16284,
		SharedBlocksHit:     104563,
		SharedBlocksRead:    3,
		SharedBlocksDirtied: 25,
		SharedBlocksWritten: 991,
		LocalBlocksHit:      1,
		LocalBlocksRead:     1,
		LocalBlocksDirtied:  1,
		LocalBlocksWritten:  1,
		TempBlocksRead:      1,
		TempBlocksWritten:   1,
		BlockReadTime:       1,
		BlockWriteTime:      1,
	}

	previous.Aggregate(current)

	assert.Equal(t, int64(1855550508), previous.QueryId)
	assert.Equal(t, "UPDATE \"stats\" SET \"value\" = ?, \"updated_at\" = ? WHERE \"stats\".\"id\" = ?", previous.Query)
	assert.Equal(t, "/*app:worker-1*/", previous.Comment)
	assert.Equal(t, int64(32378), previous.Calls)
	assert.Equal(t, 912.7367160000011, previous.TotalTime)
	assert.Equal(t, 0.001231, previous.MinTime)
	assert.Equal(t, 5.895439, previous.MaxTime)
	assert.Equal(t, 0.0282, previous.MeanTime)
	assert.Equal(t, int64(32468), previous.Rows)
	assert.Equal(t, int64(209116), previous.SharedBlocksHit)
	assert.Equal(t, int64(3), previous.SharedBlocksRead)
	assert.Equal(t, int64(40), previous.SharedBlocksDirtied)
	assert.Equal(t, int64(1981), previous.SharedBlocksWritten)
	assert.Equal(t, int64(1), previous.LocalBlocksHit)
	assert.Equal(t, int64(1), previous.LocalBlocksRead)
	assert.Equal(t, int64(1), previous.LocalBlocksDirtied)
	assert.Equal(t, int64(1), previous.LocalBlocksWritten)
	assert.Equal(t, int64(1), previous.TempBlocksRead)
	assert.Equal(t, int64(1), previous.TempBlocksWritten)
	assert.Equal(t, 5.948159, previous.BlockReadTime)
	assert.Equal(t, 1.0, previous.BlockWriteTime)
}

func TestFilterStatsMissingQuery(t *testing.T) {
	monitor := QueryStatsMonitor{
		queryStatsState:   nil,
		queryStatsChannel: nil,
	}

	queryStats := []*QueryStats{
		{
			Query: MissingQueryString,
			ServerID: &ServerID{
				Name:          "GREEN",
				ConfigVarName: "GREEN_URL",
				Database:      "foo",
			},
		},
	}

	filtered := monitor.FilterStats(queryStats)

	assert.Equal(t, 1, len(filtered))
	stats := filtered[0]
	assert.Equal(t, MissingQueryString, stats.Query)
	assert.Equal(t, "GREEN", stats.ServerID.Name)
}

func TestRedact(t *testing.T) {
	monitor := QueryStatsMonitor{
		queryStatsState:   nil,
		queryStatsChannel: nil,
	}

	assert.Equal(t, "SET application_name='sidekiq 1.2.3 app [8 of 16 busy] - REDACTED:60036'", monitor.Redact("SET application_name='sidekiq 1.2.3 app [8 of 16 busy] - 123.456.123.123:60036'"))
	assert.Equal(t, "UPDATE \"stats\" SET \"value\" = $1, \"updated_at\" = $2 WHERE \"stats\".\"id\" = $3 /*app:worker-1*/", monitor.Redact("UPDATE \"stats\" SET \"value\" = $1, \"updated_at\" = $2 WHERE \"stats\".\"id\" = $3 /*app:worker-1*/"))
}

func TestMonitorAggregateNoDupes(t *testing.T) {
	monitor := QueryStatsMonitor{
		queryStatsState:   nil,
		queryStatsChannel: nil,
	}

	queryStats := []*QueryStats{
		{
			Fingerprint: "abc123",
			Query:       "query",
			Calls:       1,
		},
		{
			Fingerprint: "abc124",
			Query:       "query2",
			Calls:       1,
		},
	}

	aggregated := monitor.AggregateStats(queryStats)

	assert.Equal(t, queryStats, aggregated)
}

func TestMonitorAggregate(t *testing.T) {
	monitor := QueryStatsMonitor{
		queryStatsState:   nil,
		queryStatsChannel: nil,
	}

	queryStats := []*QueryStats{
		{
			Fingerprint: "abc123",
			Query:       "query",
			Calls:       1,
		},
		{
			Fingerprint: "abc124",
			Query:       "query2",
			Calls:       1,
		},
		{
			Fingerprint: "abc124",
			Query:       "query2",
			Calls:       2,
			TotalTime:   5,
		},
	}

	aggregated := monitor.AggregateStats(queryStats)

	expected := []*QueryStats{
		{
			Fingerprint: "abc123",
			Query:       "query",
			Calls:       1,
		},
		{
			Fingerprint: "abc124",
			Query:       "query2",
			Calls:       3,
			TotalTime:   5,
			MeanTime:    1.6667,
		},
	}

	assert.Equal(t, expected[0], aggregated[0])
	assert.Equal(t, expected[1], aggregated[1])
}

func TestCleanQuery(t *testing.T) {
	assert.Equal(t, "select * from foo;", CleanQuery("select *\tfrom\nfoo;"))
	assert.Equal(t, "select * from foo;", CleanQuery("select * from \t\t \n foo;"))
	assert.Equal(t, "select * from foo;", CleanQuery("\tselect * from foo;\n"))
	assert.Equal(t, "select * from foo;", CleanQuery("select     *     from       foo;    "))
}
