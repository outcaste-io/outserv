// Copyright 2022 Outcaste LLC. Licensed under the Apache License v2.0.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

var url = flag.String("url", "", "Bor JSON-RPC URL")
var dst = flag.String("dst", "", "Outserv GraphQL endpoint, or a directory."+
	" If directory, it must exist already.")
var start = flag.Int64("start", 1, "Starting Block Number")
var end = flag.Int64("end", 0, "If set, stop at this Block Number, including it")
var gor = flag.Int("j", 4, "Num Goroutines to use")
var dryRun = flag.Bool("dry", false, "If true, don't send txns to GraphQL endpoint")

func uid(writer io.Writer, typ, id string) string {
	if writer == nil {
		return ""
	}
	return fmt.Sprintf("_:%s.%s", typ, id)
}

func sendRequest(data []byte) error {
	if *dryRun {
		return nil
	}
	var wr WResp
	resp, err := http.Post(fmt.Sprintf("%s/graphql", *dst),
		"application/graphql", bytes.NewBuffer(data))
	if err != nil {
		return errors.Wrapf(err, "while posting request")
	}
	out, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if err := json.Unmarshal(out, &wr); err != nil {
		return errors.Wrapf(err, "response: %s\n", out)
	}
	for _, werr := range wr.Errors {
		if len(werr.Message) > 0 {
			fmt.Printf("REQUEST was:\n%s\n", data)
			return fmt.Errorf("Got error from GraphQL: %s\n", werr.Message)
		}
	}

	return nil
}

func fetchReceipt(dst *TransactionOut, writer io.Writer) error {
	q := fmt.Sprintf(`{
	"jsonrpc": "2.0",
	"method": "eth_getTransactionReceipt",
	"params": [%q],
	"id": 1}`, dst.Hash)

	resp, err := http.Post(*url, "application/json", strings.NewReader(q))
	if err != nil {
		return errors.Wrapf(err, "Unable to fetch receipt: %s", dst.Hash)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "Unable to read response")
	}
	if len(data) == 0 {
		return fmt.Errorf("Zero length response for receipt: %s", dst.Hash)
	}

	type T struct {
		Result TransactionRPC `json:"result"`
	}
	var t T
	if err := json.Unmarshal(data, &t); err != nil {
		return errors.Wrapf(err, "unmarshal failed for receipt %s with data %s", dst.Hash, data)
	}

	src := t.Result
	if len(src.TransactionHash) == 0 {
		return fmt.Errorf("Got NO receipt for HASH: %s\n", dst.Hash)
	}
	if src.TransactionHash != dst.Hash {
		log.Fatalf("Receipt hash: %s . Wanted hash: %s\n", src.TransactionHash, dst.Hash)
		return nil
	}

	Assert(len(dst.Logs) == 0) // Should only fill once.
	for _, l := range src.Logs {
		var lo LogOut
		lo.Log = l.Log
		lo.Uid = uid(writer, "Log", fmt.Sprintf("%s-%s", dst.Hash, l.LogIndex))
		if writer != nil {
			lo.Transaction = &TransactionOut{Uid: dst.Uid}
			lo.Block = &BlockOut{Uid: uid(writer, "Block", src.BlockHash)}
		}
		dst.Logs = append(dst.Logs, lo)
	}
	dst.ContractAddress = src.ContractAddress
	dst.CumulativeGasUsed = src.CumulativeGasUsed
	// dst.EffectiveGasPrice = src.EffectiveGasPrice // Same as GasPrice
	dst.GasUsed = src.GasUsed
	dst.Status = src.Status

	// Calculate Fee
	if price, ok := new(big.Int).SetString(dst.GasPrice, 0); ok {
		if gasUsed, ok2 := new(big.Int).SetString(dst.GasUsed, 0); ok2 {
			fee := new(big.Int).Mul(price, gasUsed)
			dst.Fee = "0x" + fee.Text(16)
		}
	}
	return nil
}

func fetchBlock(writer io.Writer, blockNumber int64) (*BlockOut, error) {
	// fmt.Printf("Processing block: %d\n", blockNumber)
	q := fmt.Sprintf(`{
	"jsonrpc": "2.0",
	"method": "eth_getBlockByNumber",
	"params": ["%#x",true],
	"id": 1}`, blockNumber)

	resp, err := http.Post(*url, "application/json", strings.NewReader(q))
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to fetch block: %d", blockNumber)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read response")
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("Zero length response for block: %#x", blockNumber)
	}

	type T struct {
		Result BlockRPC `json:"result"`
	}
	var t T
	if err := json.Unmarshal(data, &t); err != nil {
		fmt.Printf("Unable to unmarshal data:\n%s\n", data)
		Check(err)
	}

	var out BlockOut
	out.Block = t.Result.Block
	out.Uid = uid(writer, "Block", out.Hash)
	if len(t.Result.Miner) > 0 {
		out.Miner = &AccountOut{
			Uid:     uid(writer, "Account", t.Result.Miner),
			Address: t.Result.Miner,
		}
	}
	for _, t := range t.Result.Transactions {
		var outTxn TransactionOut
		outTxn.Transaction = t.Transaction
		outTxn.From = &AccountOut{
			Uid:     uid(writer, "Account", t.From),
			Address: t.From,
		}
		outTxn.To = &AccountOut{
			Uid:     uid(writer, "Account", t.To),
			Address: t.To,
		}
		outTxn.Uid = uid(writer, "Transaction", outTxn.Hash)
		for i := 0; i < 60; i++ {
			err = fetchReceipt(&outTxn, writer)
			if err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		Check(err)
		out.Transactions = append(out.Transactions, outTxn)
	}
	return &out, nil
}

var blockMu = `
mutation {
	addBlock(upsert: true, input: %s) {
		numUids
	}
}
`

var blockMuWithVar = `
mutation($Blks: [AddBlockInput!]!) {
	addBlock(upsert: true, input: $Blks) {
		numUids
	}
}
`

type Resp struct {
	NumUids int `json:"numUids"`
}
type DataResp struct {
	Resp Resp `json:"addTxn"`
}
type ErrorResp struct {
	Message string `json:"message"`
}
type WResp struct {
	Data   DataResp    `json:"data"`
	Errors []ErrorResp `json:"errors"`
}
type Batch struct {
	Blks []BlockOut `json:"Blks,omitempty"`
}
type GQL struct {
	Query     string `json:"query"`
	Variables Batch  `json:"variables"`
}

func processBlock(gid int, wg *sync.WaitGroup) {
	defer wg.Done()

	var writer io.Writer
	fi, err := os.Stat(*dst)
	if err != nil {
		// This must be GraphQL endpoint.
	} else if fi.IsDir() {
		f, err := os.Create(filepath.Join(*dst, fmt.Sprintf("%02d.json.gz", gid)))
		Check(err)
		gw := gzip.NewWriter(f)
		writer = gw
		defer func() {
			gw.Flush()
			gw.Close()
			f.Sync()
			f.Close()
		}()
	}

	for num := range blockCh {
		var block *BlockOut
		for i := 0; i < 60; i++ {
			block, err = fetchBlock(writer, num)
			if err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			fmt.Printf("Unable to fetch block: %#x . Got error: %v", num, err)
			Check(err)
		}
		if writer != nil {
			data, err := json.Marshal([]BlockOut{*block})
			Check(err)
			_, err = writer.Write(data)
			Check(err)
		} else {
			// data, err := json.Marshal([]BlockOut{*block})
			// Check(err)
			// fmt.Printf("DATA:\n%s\n", data)
			q := GQL{
				Query:     blockMuWithVar,
				Variables: Batch{Blks: []BlockOut{*block}},
			}
			data, err = json.Marshal(q)
			Check(err)
			Check(sendRequest(data))
		}
		atomic.AddUint64(&numBlocks, 1)
		atomic.AddUint64(&numTxns, uint64(len(block.Transactions)))
	}
}

func Check(err error) {
	if err != nil {
		debug.PrintStack()
		log.Fatalf("Got error: %v", err)
	}
}
func Assert(thing bool) {
	if !thing {
		debug.PrintStack()
		log.Fatal("Assertion failed")
	}
}

func latestBlock() (int64, error) {
	q := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	resp, err := http.Post(*url, "application/json", strings.NewReader(q))
	if err != nil {
		return 0, errors.Wrapf(err, "while querying for latest block")
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.Wrapf(err, "Unable to read response")
	}
	type T struct {
		Result string `json:"result"`
	}
	var t T
	if err := json.Unmarshal(data, &t); err != nil {
		return 0, errors.Wrapf(err, "while unmarshal")
	}
	return strconv.ParseInt(t.Result, 0, 64)
}

var blockCh = make(chan int64, 16)
var curBlock int64
var numBlocks, numTxns uint64

func printMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	rm := NewRateMonitor(300)
	start := time.Now()
	for range ticker.C {
		maxBlockId := atomic.LoadInt64(&curBlock)
		num := atomic.LoadUint64(&numBlocks)
		rm.Capture(num)

		dur := time.Since(start)
		fmt.Printf("BlockId: %8d Processed: %5d blocks | %5d txns [ %6s @ %5d blocks/min ]\n",
			maxBlockId, num, atomic.LoadUint64(&numTxns),
			dur.Round(time.Second), rm.Rate()*60.0)
	}
}

func main() {
	flag.Parse()

	bend, err := latestBlock()
	Check(err)
	fmt.Printf("Latest block: %d\n", bend)

	wg := &sync.WaitGroup{}
	for i := 0; i < *gor; i++ {
		wg.Add(1)
		go processBlock(i, wg)
	}

	go printMetrics()
	atomic.StoreInt64(&curBlock, *start)
	for {
		cur := atomic.LoadInt64(&curBlock)
		if *end != 0 {
			if cur > *end {
				break
			}
		}

		if cur <= bend {
			blockCh <- cur
			atomic.AddInt64(&curBlock, 1)
		} else {
			time.Sleep(time.Second)
			bend, err = latestBlock()
			Check(err)
		}
	}
	close(blockCh)
	wg.Wait()
}
