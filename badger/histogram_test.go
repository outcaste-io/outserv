// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package badger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildKeyValueSizeHistogram(t *testing.T) {
	t.Run("All same size key-values", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			entries := int64(40)
			wb := db.NewWriteBatch()
			for i := rune(0); i < rune(entries); i++ {
				err := wb.SetEntryAt(NewEntry([]byte(string(i)), []byte("B")), 1)
				require.NoError(t, err)
			}
			require.NoError(t, wb.Flush())

			histogram := db.buildHistogram(nil)
			keyHistogram := histogram.keySizeHistogram
			valueHistogram := histogram.valueSizeHistogram

			require.Equal(t, entries, keyHistogram.totalCount)
			require.Equal(t, entries, valueHistogram.totalCount)

			// Each entry is of size one. So the sum of sizes should be the same
			// as number of entries
			require.Equal(t, entries, valueHistogram.sum)
			require.Equal(t, entries, keyHistogram.sum)

			// All value sizes are same. The first bin should have all the values.
			require.Equal(t, entries, valueHistogram.countPerBin[0])
			require.Equal(t, entries, keyHistogram.countPerBin[0])

			require.Equal(t, int64(1), keyHistogram.max)
			require.Equal(t, int64(1), keyHistogram.min)
			require.Equal(t, int64(1), valueHistogram.max)
			require.Equal(t, int64(1), valueHistogram.min)
		})
	})

	t.Run("different size key-values", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			entries := int64(3)
			wb := db.NewWriteBatch()
			err := wb.SetEntryAt(NewEntry([]byte("A"), []byte("B")), 1)
			require.NoError(t, err)
			err = wb.SetEntryAt(NewEntry([]byte("AA"), []byte("BB")), 1)
			require.NoError(t, err)

			err = wb.SetEntryAt(NewEntry([]byte("AAA"), []byte("BBB")), 1)
			require.NoError(t, err)
			require.NoError(t, wb.Flush())

			histogram := db.buildHistogram(nil)
			keyHistogram := histogram.keySizeHistogram
			valueHistogram := histogram.valueSizeHistogram

			require.Equal(t, entries, keyHistogram.totalCount)
			require.Equal(t, entries, valueHistogram.totalCount)

			// Each entry is of size one. So the sum of sizes should be the same
			// as number of entries
			require.Equal(t, int64(6), valueHistogram.sum)
			require.Equal(t, int64(6), keyHistogram.sum)

			// Length 1 key is in first bucket, length 2 and 3 are in the second
			// bucket
			require.Equal(t, int64(1), valueHistogram.countPerBin[0])
			require.Equal(t, int64(2), valueHistogram.countPerBin[1])
			require.Equal(t, int64(1), keyHistogram.countPerBin[0])
			require.Equal(t, int64(2), keyHistogram.countPerBin[1])

			require.Equal(t, int64(3), keyHistogram.max)
			require.Equal(t, int64(1), keyHistogram.min)
			require.Equal(t, int64(3), valueHistogram.max)
			require.Equal(t, int64(1), valueHistogram.min)
		})
	})
}
