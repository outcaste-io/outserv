package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

var root = flag.String("datadir", "", "Root ETH dir")
var startBlock = flag.Uint64("start", 0, "Start block")
var endBlock = flag.Uint64("end", 15000000, "End block")
var out = flag.String("out", "", "Output Directory")

func Check(err error) {
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
}

func main() {
	flag.Parse()

	ldb := filepath.Join(*root, "geth/chaindata")
	ancient := filepath.Join(*root, "geth/chaindata/ancient")
	db, err := rawdb.NewLevelDBDatabaseWithFreezer(ldb, 2048, 524288, ancient, "eth/db/chaindata/", true)
	Check(err)

	var (
		count  int64
		start  = time.Now()
		logged = time.Now()
	)

	itr := db.NewIterator(nil, nil)
	defer itr.Release()

	for itr.Next() {
		key := itr.Key()
		switch {
		case bytes.HasPrefix(key, headerPrefix) && len(key) == (len(headerPrefix)+8+common.HashLength):
			val := itr.Value()
			h := new(types.Header)
			Check(rlp.DecodeBytes(val, &h))
			data, err := json.Marshal(h)
			Check(err)
			fmt.Printf("Header:\n%s\n", data)
		case bytes.HasPrefix(key, blockBodyPrefix) && len(key) == (len(blockBodyPrefix)+8+common.HashLength):
			val := itr.Value()
			b := new(types.Body)
			Check(rlp.DecodeBytes(val, &b))
			data, err := json.Marshal(b)
			Check(err)
			fmt.Printf("Body:\n%s\n", data)
		case bytes.HasPrefix(key, blockReceiptsPrefix) && len(key) == (len(blockReceiptsPrefix)+8+common.HashLength):
			val := itr.Value()
			var r []*types.ReceiptForStorage
			Check(rlp.DecodeBytes(val, &r))
			data, err := json.Marshal(r)
			Check(err)
			fmt.Printf("Receipts:\n%s\n", data)
		default:
			// pass
		}

		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			fmt.Printf("Inspecting database | count: %d | elapsed: %s\n", count, common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
		break // HACK
	}

	f, err := os.Create(filepath.Join(*out, "receipts.json.gz"))
	Check(err)
	_ = f
	writer := bufio.NewWriterSize(f, 16<<20)
	gzWriter := gzip.NewWriter(writer)

	defer func() {
		Check(gzWriter.Flush())
		Check(writer.Flush())
		Check(f.Sync())
		Check(f.Close())
	}()
	// Inspect append-only file store then.
	// for _, category := range []string{freezerHeaderTable, freezerBodiesTable, freezerReceiptTable, freezerHashTable, freezerDifficultyTable} {
	err = db.ReadAncients(func(op ethdb.AncientReaderOp) error {
		count := 0
		for curBlock := *startBlock; curBlock < *endBlock; curBlock++ {
			fmt.Printf("Block: %d\n", curBlock)

			var h types.Header
			var r types.Body
			var rs []*types.ReceiptForStorage
			if false {
				val, err := op.Ancient(freezerHeaderTable, curBlock)
				Check(err)
				Check(rlp.DecodeBytes(val, &h))
				data, err := json.Marshal(h)
				Check(err)
				_ = data
				// fmt.Printf("Header:\n%s\n", data)
			}
			if false {
				val, err := op.Ancient(freezerBodiesTable, curBlock)
				Check(err)
				Check(rlp.DecodeBytes(val, &r))
				data, err := json.Marshal(r)
				Check(err)
				_ = data
				// fmt.Printf("Body:\n%s\n", data)
			}
			if true {
				val, err := op.Ancient(freezerReceiptTable, curBlock)
				Check(err)
				Check(rlp.DecodeBytes(val, &rs))
				logIndex := uint(0)
				for i := 0; i < len(rs); i++ {
					for j := 0; j < len(rs[i].Logs); j++ {
						rs[i].Logs[j].BlockNumber = curBlock
						rs[i].Logs[j].BlockHash = h.Hash()
						rs[i].Logs[j].TxHash = r.Transactions[i].Hash()
						rs[i].Logs[j].TxIndex = uint(i)
						rs[i].Logs[j].Index = logIndex
						logIndex++
					}
				}
				data, err := json.Marshal(rs)
				Check(err)
				_, err = gzWriter.Write(data)
				Check(err)
				// fmt.Printf("Receipts:\n%s\n", data)
			}

			count++
			if count > 10 {
				return nil
			}
		}
		return nil
	})
	Check(err)
	// }
	fmt.Println("DONE")
}
