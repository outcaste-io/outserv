// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package badger

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueEntryChecksum(t *testing.T) {
	k := []byte("KEY")
	v := []byte(fmt.Sprintf("val%100d", 10))
	t.Run("ok", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		opt := getTestOptions(dir)
		opt.VerifyValueChecksum = true
		db, err := Open(opt)
		require.NoError(t, err)

		txnSetSlow(t, db, k, v, 0)
		require.NoError(t, db.Close())

		db, err = Open(opt)
		require.NoError(t, err)

		txn := db.NewReadTxn(1)
		entry, err := txn.Get(k)
		require.NoError(t, err)

		x, err := entry.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, v, x)

		require.NoError(t, db.Close())
	})
}
