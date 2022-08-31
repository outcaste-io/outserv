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
	"github.com/outcaste-io/outserv/x"
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

type Block struct {
	types.Header
	Transactions json.RawMessage `json:"transactions"`
	Uncles       json.RawMessage `json:"uncles"`
	// Transactions []*types.Transaction
	// Uncles       []*types.Header
	Receipts json.RawMessage `json:"receipts"`
	// Receipts []*types.ReceiptForStorage
}

type Writer struct {
	gw      *gzip.Writer
	bw      *bufio.Writer
	fd      *os.File
	written uint64
}

func (w *Writer) Write(data []byte) {
	n, err := w.gw.Write(data)
	w.written += uint64(n)
	Check(err)
}
func (w *Writer) Close() error {
	Check(w.gw.Close())
	Check(w.bw.Flush())
	Check(w.fd.Sync())
	Check(w.fd.Close())
	return nil
}
func NewWriter(fileName string) *Writer {
	w := &Writer{}
	var err error
	w.fd, err = os.Create(fileName)
	x.Check(err)
	fmt.Printf("Created file: %s\n", w.fd.Name())

	w.bw = bufio.NewWriterSize(w.fd, 4<<20)
	w.gw, err = gzip.NewWriterLevel(w.bw, gzip.BestCompression)
	x.Check(err)
	return w
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
		if count%1000 == 0 || time.Since(logged) > 8*time.Second {
			fmt.Printf("Inspecting database | count: %d | elapsed: %s\n", count, common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
		break // HACK
	}

	receiptWriter := NewWriter(filepath.Join(*out, "receipts.json.gz"))
	defer receiptWriter.Close()

	headerWriter := NewWriter(filepath.Join(*out, "headers.json.gz"))
	defer headerWriter.Close()

	bodyWriter := NewWriter(filepath.Join(*out, "bodies.json.gz"))
	defer bodyWriter.Close()

	// Inspect append-only file store then.
	// for _, category := range []string{freezerHeaderTable, freezerBodiesTable, freezerReceiptTable, freezerHashTable, freezerDifficultyTable} {
	err = db.ReadAncients(func(op ethdb.AncientReaderOp) error {
		count := 0
		for curBlock := *startBlock; curBlock < *endBlock; curBlock++ {
			fmt.Printf("Block: %d\n", curBlock)

			// block := new(Block)
			var h types.Header
			var r types.Body
			var rs []*types.ReceiptForStorage
			if true {
				val, err := op.Ancient(freezerHeaderTable, curBlock)
				Check(err)
				Check(rlp.DecodeBytes(val, &h))
				data, err := json.Marshal(h)
				Check(err)
				headerWriter.Write(data)
				// block.Header = h
				// fmt.Printf("Header:\n%s\n", data)
			}
			if true {
				val, err := op.Ancient(freezerBodiesTable, curBlock)
				Check(err)
				Check(rlp.DecodeBytes(val, &r))

				data, err := json.Marshal(r)
				Check(err)
				fmt.Printf("body:\n%s\n", data[0:100])
				data = bytes.ReplaceAll(data, []byte(`"Transactions":`), []byte(`"transactions":`))
				data = bytes.ReplaceAll(data, []byte(`"Uncles":`), []byte(`"uncles":`))
				if data[0] == '{' {
					bl := fmt.Sprintf(`{"hash": %q, `, h.Hash())
					dst := append([]byte(bl), data[1:]...)
					data = dst
					fmt.Printf("data:\n%s\n", data[:100])
				} else {
					log.Fatalf("Invalid data: %s\n", data)
				}
				bodyWriter.Write(data)

				// type B struct {
				// 	Transactions json.RawMessage `json:"Transactions,omitempty"`
				// 	Uncles       json.RawMessage `json:"Uncles,omitempty"`
				// }
				// var tmp B
				// Check(json.Unmarshal(data, &tmp))
				// fmt.Printf("Got tmp\n%+v\n", tmp)

				// block.Transactions = tmp.Transactions
				// block.Uncles = tmp.Uncles
				// data, err = json.Marshal(block)
				// Check(err)
				// fmt.Printf("Got block\n%s\n", data)

				// data, err = json.Marshal(r.Transactions)
				// Check(err)
				// block.Transactions = data
				// data, err = json.Marshal(r.Uncles)
				// Check(err)
				// block.Uncles = data
				// _, err = gzWriter.Write(data)
				// Check(err)
				// fmt.Printf("Body:\n%s\n", data)
				// block.Body = &r
				// block.Transactions = r.Transactions
				// block.Uncles = r.Uncles
			}
			if true {
				val, err := op.Ancient(freezerReceiptTable, curBlock)
				Check(err)
				Check(rlp.DecodeBytes(val, &rs))
				logIndex := uint(0)
				if len(r.Transactions) != len(rs) {
					panic(fmt.Sprintf("len txns: %d len(rs): %d. Mismatch\n", len(r.Transactions), len(rs)))
				}
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
				receiptWriter.Write(data)
				// block.Receipts = data
				// _, err = gzWriter.Write(data)
				// Check(err)
				// fmt.Printf("Receipts:\n%s\n", data)
			}
			// data, err := json.Marshal(block)
			// Check(err)
			// _, err = gzWriter.Write(data)
			// Check(err)

			count++
			if count >= 1 {
				return nil
			}
		}
		return nil
	})
	Check(err)
	// }
	fmt.Println("DONE")
}
