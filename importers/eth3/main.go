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
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/outserv/x"
)

var root = flag.String("datadir", "", "Root ETH dir")
var startBlock = flag.Uint64("start", 0, "Start block")
var endBlock = flag.Uint64("end", 15000000, "End block")
var out = flag.String("out", "", "Output Directory")
var numGo = flag.Int("gor", 8, "Number of goroutines")

func Check(err error) {
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
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

func parseBody(dst *BlockOut, b *types.Body) {
	cc := params.MainnetChainConfig
	bNum, err := hexutil.DecodeBig(dst.Number)
	Check(err)
	for i, tx := range b.Transactions {
		data, err := json.Marshal(tx)
		Check(err)
		var txn TransactionIn
		Check(json.Unmarshal(data, &txn))

		// Fill in the fields which won't be present.
		signer := types.MakeSigner(cc, bNum)
		from, _ := types.Sender(signer, tx)
		txn.From = from.Hex()
		txn.BlockNumber = dst.Number
		txn.TransactionIndex = hexutil.Uint(i).String()

		txout := &TransactionOut{Transaction: txn.Transaction}
		txout.From = &Account{Address: strings.ToLower(txn.From)}
		txout.To = &Account{Address: strings.ToLower(txn.To)}
		dst.Transactions = append(dst.Transactions, txout)
	}

	// Marshal each uncle and parse back to Block struct.
	udata, err := json.Marshal(b.Uncles)
	Check(err)
	dst.Ommers = udata
	dst.OmmerCount = hexutil.Uint(len(b.Uncles)).String()
}

var nilAddr = make([]byte, common.AddressLength)

func parseReceipts(out *BlockOut, rs []*types.ReceiptForStorage) {
	if len(out.Transactions) != len(rs) {
		panic(fmt.Sprintf("len txns: %d len(rs): %d. Mismatch\n", len(out.Transactions), len(rs)))
	}
	var cumGas uint64
	var logIndex uint
	for i := 0; i < len(rs); i++ {
		txn := out.Transactions[i]
		dst := rs[i]
		if !bytes.Equal(dst.ContractAddress.Bytes(), nilAddr) {
			txn.ContractAddress = dst.ContractAddress.Hex()
		} // else nil
		txn.Root = hexutil.Bytes(dst.PostState).String()
		txn.CumulativeGasUsed = hexutil.Uint64(dst.CumulativeGasUsed).String()
		txn.GasUsed = hexutil.Uint64(dst.CumulativeGasUsed - cumGas).String()
		cumGas = dst.CumulativeGasUsed
		txn.Status = hexutil.Uint64(dst.Status).String()
		txn.Hash = out.Transactions[i].Hash
		txn.LogsBloom = hexutil.Bytes(dst.Bloom.Bytes()).String()

		for j := 0; j < len(rs[i].Logs); j++ {
			lj := dst.Logs[j]
			data, err := json.Marshal(lj)
			Check(err)

			var log Log
			Check(json.Unmarshal(data, &log))

			log.Lid = fmt.Sprintf("%s|%d", out.Hash, logIndex)
			log.BlockNumber = out.Number
			log.TransactionIndex = hexutil.Uint(i).String()
			log.LogIndex = hexutil.Uint(logIndex).String()
			logIndex++
			txn.Logs = append(txn.Logs, log)
			out.Logs = append(out.Logs, Log{Lid: log.Lid}) // Just a ref is sufficient.
		}
		out.Transactions = append(out.Transactions, txn)
	}
}

// processAncients would output both start and end block.
func processAncients(th *y.Throttle, db ethdb.Database, startBlock, endBlock uint64) {
	defer th.Done(nil)

	oneWriter := NewWriter(filepath.Join(*out, fmt.Sprintf("data-%04d.json.gz", endBlock/Width)))
	defer oneWriter.Close()

	handleBlock := func(op ethdb.AncientReaderOp, curBlock uint64) {
		// fmt.Printf("Block: %d\n", curBlock)
		atomic.AddUint64(&numBlocks, 1)

		var h types.Header
		var b types.Body
		var rs []*types.ReceiptForStorage

		// We can check this, if needed. But, the error handling in
		// op.Ancient calls is sufficient.
		if false {
			has1, err := op.HasAncient(freezerHeaderTable, curBlock)
			Check(err)
			has2, err := op.HasAncient(freezerBodiesTable, curBlock)
			Check(err)
			has3, err := op.HasAncient(freezerReceiptTable, curBlock)
			Check(err)
			if !(has1 && has2 && has3) {
				fmt.Printf("---> Didn't find block: %d\n", curBlock)
				os.Exit(1)
			}
		}

		block := new(BlockOut)
		{
			val, err := op.Ancient(freezerHeaderTable, curBlock)
			Check(err)
			Check(rlp.DecodeBytes(val, &h))

			data, err := json.Marshal(h)
			Check(err)
			var b BlockIn
			Check(json.Unmarshal(data, &b))
			block.Block = b.Block
			if len(b.Miner) > 0 {
				block.Miner = &Account{Address: b.Miner}
			}
		}
		{
			val, err := op.Ancient(freezerBodiesTable, curBlock)
			Check(err)
			Check(rlp.DecodeBytes(val, &b))
			parseBody(block, &b)
		}
		{
			val, err := op.Ancient(freezerReceiptTable, curBlock)
			Check(err)
			Check(rlp.DecodeBytes(val, &rs))
			parseReceipts(block, rs)
		}
		data, err := json.Marshal(block)
		Check(err)
		oneWriter.Write(data)
	}

	err := db.ReadAncients(func(op ethdb.AncientReaderOp) error {
		oneWriter.Write([]byte(`[`))
		for curBlock := startBlock; curBlock <= endBlock; curBlock++ {
			handleBlock(op, curBlock)
			if curBlock != endBlock {
				oneWriter.Write([]byte(`,`))
			}
		}
		oneWriter.Write([]byte(`]`))
		return nil
	})
	Check(err)
}

var numBlocks uint64

func printMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	rm := y.NewRateMonitor(300)
	start := time.Now()
	for range ticker.C {
		numBlocks := atomic.LoadUint64(&numBlocks)
		rm.Capture(numBlocks)
		fmt.Printf("Processed %8d blocks | Elapsed: %6s | %5d blocks/min\n",
			numBlocks, time.Since(start).Truncate(time.Second),
			rm.Rate()*60.0)
	}
}

func runIteratorXXX(db ethdb.Database) {
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
	}
}

const Width = uint64(100000)

func main() {
	flag.Parse()

	ldb := filepath.Join(*root, "geth/chaindata")
	ancient := filepath.Join(*root, "geth/chaindata/ancient")
	db, err := rawdb.NewLevelDBDatabaseWithFreezer(ldb, 2048, 524288, ancient, "eth/db/chaindata/", true)
	Check(err)

	go printMetrics()

	th := y.NewThrottle(*numGo)
	for i := *startBlock; i < *endBlock; {
		Check(th.Do())
		go processAncients(th, db, i+1, i+Width)
		i += Width
	}
	th.Finish()
	fmt.Println("DONE")
}
