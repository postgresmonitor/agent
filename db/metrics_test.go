package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseStatsDelta(t *testing.T) {
	stats := &DatabaseStats{
		CommittedTransactions:  4153098541,
		RollbackedTransactions: 57603,
		BlocksRead:             2067421652,
		BlocksHit:              342430835992,
		RowsReturned:           569869119387,
		RowsFetched:            226772290420,
		RowsInserted:           1149620737,
		RowsUpdated:            1042048950,
		RowsDeleted:            1149768523,
		Conflicts:              0,
		TempFiles:              0,
		TempBytes:              0,
		Deadlocks:              198,
		BlockReadTime:          85194502.642,
		BlockWriteTime:         671848.036,
	}

	v2 := &DatabaseStats{
		CommittedTransactions:  4153164646,
		RollbackedTransactions: 57603,
		BlocksRead:             2067446094,
		BlocksHit:              342435163707,
		RowsReturned:           569878155818,
		RowsFetched:            226775613836,
		RowsInserted:           1149636463,
		RowsUpdated:            1042064660,
		RowsDeleted:            1149768524,
		Conflicts:              0,
		TempFiles:              0,
		TempBytes:              0,
		Deadlocks:              198,
		BlockReadTime:          85194708.609,
		BlockWriteTime:         671848.036,
	}

	delta := stats.Delta(v2)

	assert.Equal(t, 66105.0, delta.CommittedTransactions)
	assert.Equal(t, 0.0, delta.RollbackedTransactions)
	assert.Equal(t, 24442.0, delta.BlocksRead)
	assert.Equal(t, 4327715.0, delta.BlocksHit)
	assert.Equal(t, 9036431.0, delta.RowsReturned)
	assert.Equal(t, 3323416.0, delta.RowsFetched)
	assert.Equal(t, 15726.0, delta.RowsInserted)
	assert.Equal(t, 15710.0, delta.RowsUpdated)
	assert.Equal(t, 1.0, delta.RowsDeleted)
	assert.Equal(t, 0.0, delta.Conflicts)
	assert.Equal(t, 0.0, delta.TempFiles)
	assert.Equal(t, 0.0, delta.TempBytes)
	assert.Equal(t, 0.0, delta.Deadlocks)
	assert.Equal(t, 205.96699999272823, delta.BlockReadTime)
	assert.Equal(t, 0.0, delta.BlockWriteTime)
}
