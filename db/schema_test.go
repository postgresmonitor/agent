package db

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableDelta(t *testing.T) {
	p := &Table{
		Name:                     "table",
		Schema:                   "schema",
		TotalBytes:               1000,
		TotalBytesTotal:          2000,
		IndexBytes:               500,
		IndexBytesTotal:          5000,
		ToastBytes:               1000,
		ToastBytesTotal:          1000,
		TableBytes:               1000,
		TableBytesTotal:          1000,
		BloatBytes:               1000,
		BloatBytesTotal:          1000,
		BloatFactor:              0.2,
		SequentialScans:          10000,
		SequentialScanReadRows:   10000,
		IndexScans:               10000,
		IndexScanReadRows:        10000,
		InsertedRows:             10000,
		UpdatedRows:              10000,
		DeletedRows:              10000,
		LiveRowEstimate:          10000,
		LiveRowEstimateTotal:     10000,
		DeadRowEstimate:          10000,
		DeadRowEstimateTotal:     10000,
		ModifiedRowsSinceAnalyze: 10000,
		LastVacuumAt:             sql.NullInt64{Valid: true, Int64: 1000},
		LastAutovacuumAt:         sql.NullInt64{Valid: true, Int64: 1000},
		LastAnalyzeAt:            sql.NullInt64{Valid: true, Int64: 1000},
		LastAutoanalyzeAt:        sql.NullInt64{Valid: true, Int64: 1000},
		VacuumCount:              10000,
		AutovacuumCount:          10000,
		AnalyzeCount:             10000,
		AutoanalyzeCount:         10000,
		DiskBlocksRead:           10000,
		DiskBlocksHit:            10000,
		DiskIndexBlocksRead:      10000,
		DiskIndexBlocksHit:       10000,
		DiskToastBlocksRead:      10000,
		DiskToastBlocksHit:       10000,
		DiskToastIndexBlocksRead: 10000,
		DiskToastIndexBlocksHit:  10000,
	}

	l := &Table{
		Name:                     "table",
		Schema:                   "schema",
		TotalBytes:               1000,
		TotalBytesTotal:          3000,
		IndexBytes:               500,
		IndexBytesTotal:          6000,
		ToastBytes:               1000,
		ToastBytesTotal:          2000,
		TableBytes:               1000,
		TableBytesTotal:          2000,
		BloatBytes:               1000,
		BloatBytesTotal:          2000,
		BloatFactor:              0.2,
		SequentialScans:          10000,
		SequentialScanReadRows:   10000,
		IndexScans:               10000,
		IndexScanReadRows:        10000,
		InsertedRows:             10000,
		UpdatedRows:              10000,
		DeletedRows:              10000,
		LiveRowEstimate:          10000,
		LiveRowEstimateTotal:     20000,
		DeadRowEstimate:          10000,
		DeadRowEstimateTotal:     20000,
		ModifiedRowsSinceAnalyze: 10000,
		LastVacuumAt:             sql.NullInt64{Valid: true, Int64: 1000},
		LastAutovacuumAt:         sql.NullInt64{Valid: true, Int64: 1000},
		LastAnalyzeAt:            sql.NullInt64{Valid: true, Int64: 1000},
		LastAutoanalyzeAt:        sql.NullInt64{Valid: true, Int64: 1000},
		VacuumCount:              10000,
		AutovacuumCount:          10000,
		AnalyzeCount:             10000,
		AutoanalyzeCount:         10000,
		DiskBlocksRead:           10000,
		DiskBlocksHit:            10000,
		DiskIndexBlocksRead:      10000,
		DiskIndexBlocksHit:       10000,
		DiskToastBlocksRead:      10000,
		DiskToastBlocksHit:       10000,
		DiskToastIndexBlocksRead: 10000,
		DiskToastIndexBlocksHit:  10000,
	}

	d := p.Delta(l)

	assert.Equal(t, "table", d.Name)
	assert.Equal(t, "schema", d.Schema)
	assert.Equal(t, int64(1000), d.TotalBytes)
	assert.Equal(t, int64(3000), d.TotalBytesTotal)
	assert.Equal(t, int64(1000), d.IndexBytes)
	assert.Equal(t, int64(6000), d.IndexBytesTotal)
	assert.Equal(t, int64(1000), d.ToastBytes)
	assert.Equal(t, int64(2000), d.ToastBytesTotal)
	assert.Equal(t, int64(1000), d.TableBytes)
	assert.Equal(t, int64(2000), d.TableBytesTotal)
	assert.Equal(t, int64(1000), d.BloatBytes)
	assert.Equal(t, int64(2000), d.BloatBytesTotal)
	assert.Equal(t, float64(0.2), d.BloatFactor)
	assert.Equal(t, int64(0), d.SequentialScans)
	assert.Equal(t, int64(0), d.SequentialScanReadRows)
	assert.Equal(t, int64(0), d.IndexScans)
	assert.Equal(t, int64(0), d.IndexScanReadRows)
	assert.Equal(t, int64(0), d.InsertedRows)
	assert.Equal(t, int64(0), d.UpdatedRows)
	assert.Equal(t, int64(0), d.DeletedRows)
	assert.Equal(t, int64(10000), d.LiveRowEstimate)
	assert.Equal(t, int64(20000), d.LiveRowEstimateTotal)
	assert.Equal(t, int64(10000), d.DeadRowEstimate)
	assert.Equal(t, int64(20000), d.DeadRowEstimateTotal)
	assert.Equal(t, int64(10000), d.ModifiedRowsSinceAnalyze)
	assert.Equal(t, sql.NullInt64{Valid: true, Int64: 1000}, d.LastVacuumAt)
	assert.Equal(t, sql.NullInt64{Valid: true, Int64: 1000}, d.LastAutovacuumAt)
	assert.Equal(t, sql.NullInt64{Valid: true, Int64: 1000}, d.LastAnalyzeAt)
	assert.Equal(t, sql.NullInt64{Valid: true, Int64: 1000}, d.LastAutoanalyzeAt)
	assert.Equal(t, int64(0), d.VacuumCount)
	assert.Equal(t, int64(0), d.AutovacuumCount)
	assert.Equal(t, int64(0), d.AnalyzeCount)
	assert.Equal(t, int64(0), d.AutoanalyzeCount)
	assert.Equal(t, int64(0), d.DiskBlocksRead)
	assert.Equal(t, int64(0), d.DiskBlocksHit)
	assert.Equal(t, int64(0), d.DiskIndexBlocksRead)
	assert.Equal(t, int64(0), d.DiskIndexBlocksHit)
	assert.Equal(t, int64(0), d.DiskToastBlocksRead)
	assert.Equal(t, int64(0), d.DiskToastBlocksHit)
	assert.Equal(t, int64(0), d.DiskToastIndexBlocksRead)
	assert.Equal(t, int64(0), d.DiskToastIndexBlocksHit)
}

func TestIndexDelta(t *testing.T) {
	i := &Index{
		Name:            "index",
		Schema:          "schema",
		TableName:       "table",
		Unique:          true,
		Unused:          false,
		Valid:           true,
		Definition:      "definition",
		Bytes:           0,
		BytesTotal:      1000,
		BloatBytes:      200,
		BloatBytesTotal: 200,
		BloatFactor:     0.2,
		Scans:           10000,
		DiskBlocksRead:  5,
		DiskBlocksHit:   10,
	}

	l := &Index{
		Name:            "index",
		Schema:          "schema",
		TableName:       "table",
		Unique:          false,
		Unused:          true,
		Valid:           false,
		Definition:      "definition2",
		Bytes:           0,
		BytesTotal:      2000,
		BloatBytes:      200,
		BloatBytesTotal: 300,
		BloatFactor:     0.3,
		Scans:           40000,
		DiskBlocksRead:  6,
		DiskBlocksHit:   100,
	}

	d := i.Delta(l)

	assert.Equal(t, "index", d.Name)
	assert.Equal(t, "schema", d.Schema)
	assert.Equal(t, "table", d.TableName)
	assert.Equal(t, false, d.Unique)
	assert.Equal(t, true, d.Unused)
	assert.Equal(t, false, d.Valid)
	assert.Equal(t, "definition2", d.Definition)

	assert.Equal(t, int64(1000), d.Bytes)
	assert.Equal(t, int64(2000), d.BytesTotal)
	assert.Equal(t, int64(100), d.BloatBytes)
	assert.Equal(t, int64(300), d.BloatBytesTotal)
	assert.Equal(t, float64(0.3), d.BloatFactor)
	assert.Equal(t, int64(30000), d.Scans)
	assert.Equal(t, int64(1), d.DiskBlocksRead)
	assert.Equal(t, int64(90), d.DiskBlocksHit)
}
