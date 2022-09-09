// Copyright 2022 Outcaste LLC. Licensed under the Apache License v2.0.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
)

var (
	graphql  = flag.String("graphql", "", "Outserv GraphQL endpoint")
	rpc      = flag.String("rpc", "", "JSON-RPC endpoint")
	gor      = flag.Int("j", 128, "Num Goroutines to use")
	minBlock = flag.Int64("min", 10000000, "Min block number")
	maxBlock = flag.Int64("max", 15300000, "Max block number")
	dur      = flag.Duration("dur", time.Minute, "How long to run the benchmark")
	all      = flag.Bool("all", false, "Retrieve all fields.")
	sample   = flag.Int("sample", 10000, "Output query and response every N times")

	blockFields     = `difficulty, extraData, gasLimit, gasUsed, hash, logsBloom, miner { address }, mixHash, nonce, number, parentHash, receiptsRoot, sha3Uncles, size, stateRoot, timestamp, totalDifficulty`
	blockFieldsMini = `gasUsed, hash, number, size, timestamp`
	txnFields       = `contractAddress, cumulativeGasUsed, from { address }, gas, gasPrice, gasUsed, hash, input, maxFeePerGas, maxPriorityFeePerGas, nonce, r, s, status, to { address }, transactionIndex, type, v, value`
	txnFieldsMini   = `from { address }, gasUsed, hash, status, to { address }`
	logFields       = `address, blockNumber, data, logIndex, removed, topics, transactionIndex`
	logFieldsMini   = `address, logIndex`
)

func getBlockQuery(number int64) string {
	const q string = `{ queryBlock(filter: {number: {eq: "%#x"}}) { %s, transactions { %s , logs { %s }}}}`
	if *all {
		return fmt.Sprintf(q, number, blockFields, txnFields, logFields)
	} else {
		return fmt.Sprintf(q, number, blockFieldsMini, txnFieldsMini, logFieldsMini)
	}
}

func fetchBlockWithTxnAndLogs(client *http.Client, blockNum int64) (int64, error) {
	q := getBlockQuery(blockNum)
	// fmt.Printf("Query: %s\n", q)

	buf := bytes.NewBufferString(q)
	req, err := http.NewRequest("POST", *graphql, buf)
	x.Check(err)
	req.Header.Add("Content-Type", "application/graphql")

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if rand.Intn(*sample) == 0 {
		fmt.Printf("Query:\n%s\n%s\n", q, data)
	}
	resp.Body.Close()

	// If we capture errors like this, we get complains about non-nullable
	// fields, for example, for this txn:
	// 0xd8c56fa18db12dfb8c6c717825ed7c02ad4a9d79f9249b3fe60570aa3b223875
	// which doesn't have a to address.
	//
	// if strings.Contains(string(data), `"errors":`) {
	// 	return 0, fmt.Errorf("%s", data)
	// }
	return int64(len(data)), nil
}

type Log struct {
	BlockNumber string `json:"blockNumber"`
}

type Txn struct {
	Hash        string
	BlockNumber string `json:"blockNumber"`
	Logs        []Log  `json:"logs"`
}

type Block struct {
	Number       string
	Transactions []Txn
}

type Error struct {
	Code    int
	Message string
}
type BlockResp struct {
	Result Block
	Error  Error
}
type TxnResp struct {
	Result Txn
	Error  Error
}

func callRPC(client *http.Client, q string, cu uint64) ([]byte, error) {
	atomic.AddUint64(&numQueries, 1)
	atomic.AddUint64(&numCUs, cu)

	for i := 0; ; i++ {
		buf := bytes.NewBufferString(q)
		req, err := http.NewRequest("POST", *rpc, buf)
		x.Check(err)
		req.Header.Add("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		ds := string(data)
		if strings.Contains(ds, `"error":`) {
			if strings.Contains(ds, `"error":{"code":429,`) {
				atomic.AddUint64(&numLimits, 1)
				// fmt.Println("Rate limited. Sleeping for a sec")
				time.Sleep(time.Second)
				continue
			}
			fmt.Printf("Got error response: %s\n", ds)
			os.Exit(1)
		}
		return data, nil
	}
}

// The CUs are derived from:
// https://docs.alchemy.com/reference/compute-units
func fetchBlockWithTxnAndLogsWithRPC(client *http.Client, blockNum int64) (int64, error) {
	hno := hexutil.EncodeUint64(uint64(blockNum))
	q := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[%q, true],"id":1}`, hno)
	// fmt.Printf("Block Query: %s\n", q)
	data, err := callRPC(client, q, 16)
	x.Check(err)
	sz := int64(len(data))

	var resp BlockResp
	if err := json.Unmarshal(data, &resp); err != nil {
		fmt.Printf("Got invalid block resp data: %s\n", data)
		os.Exit(1)
	}
	if resp.Result.Number != hno {
		fmt.Printf("Got result: %+v. Expecting: %s Test Failed.\n", resp.Result, hno)
		fmt.Printf("Response: %s\n", data)
		os.Exit(1)
	}
	for _, txn := range resp.Result.Transactions {
		if txn.BlockNumber != hno {
			fmt.Printf("Got result: %+v. Expecting: %s Test Failed.\n", resp.Result)
			fmt.Printf("Response: %s\n", data)
			os.Exit(1)
		}
		q = fmt.Sprintf(
			`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":[%q],"id":1}`, txn.Hash)
		// fmt.Printf("Receipt query: %s\n", q)
		data, err = callRPC(client, q, 15)
		x.Check(err)
		sz += int64(len(data))

		var txnResp TxnResp
		if err := json.Unmarshal(data, &txnResp); err != nil {
			fmt.Printf("Got invalid txn resp data: %s\n", data)
			os.Exit(1)
		}
		if txnResp.Result.BlockNumber != hno {
			fmt.Printf("Got result: %+v. Expecting: %s Test Failed.\n", txnResp.Result)
			fmt.Printf("Response: %s\n", data)
			os.Exit(1)
		}
		for _, log := range txnResp.Result.Logs {
			if log.BlockNumber != hno {
				fmt.Printf("Got result: %+v. Expecting: %s Test Failed.\n", txnResp.Result)
				fmt.Printf("Response: %s\n", data)
				os.Exit(1)
			}
		}
	}
	return sz, nil
}

var numBlocks, numQueries, numBytes, numLimits, numCUs uint64

func printQps() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	start := time.Now()
	rm := y.NewRateMonitor(300)
	for range ticker.C {
		numB := atomic.LoadUint64(&numBlocks)
		rm.Capture(numB)
		numQ := atomic.LoadUint64(&numQueries)
		numL := atomic.LoadUint64(&numLimits)
		numC := atomic.LoadUint64(&numCUs)
		bytes := atomic.LoadUint64(&numBytes)

		dur := time.Since(start)
		fmt.Printf("Num Blocks: %5d | Num Queries: %4d | Num 429: %4d | CUs: %4d | Data: %s [ %6s @ %d blocks/sec ]\n",
			numB, numQ, numL, numC, humanize.IBytes(bytes), dur.Round(time.Second), rm.Rate())
	}
}

func main() {
	flag.Parse()

	rand.Seed(time.Now().UnixNano())
	end := time.Now().Add(*dur)
	fmt.Printf("Time now: %s . Ending at %s\n",
		time.Now().Truncate(time.Second), end.Truncate(time.Second))

	go printQps()

	var mu sync.Mutex
	bounds := z.HistogramBounds(0, 10)
	last := float64(2048)
	for i := 0; i < 1024; i++ {
		bounds = append(bounds, last)
		last += 1024.0
	}
	fmt.Printf("Bounds are: %+v\n", bounds)
	histDur := z.NewHistogramData(bounds)
	histSz := z.NewHistogramData(bounds)

	N := *maxBlock - *minBlock
	var wg sync.WaitGroup
	for i := 0; i < *gor; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			client := &http.Client{}
			var times []int64
			var sizes []int64
			for i := int64(0); ; i++ {
				ts := time.Now()
				if ts.After(end) {
					break
				}
				bno := rand.Int63n(N) + *minBlock

				var sz int64
				var err error
				if len(*graphql) > 0 {
					sz, err = fetchBlockWithTxnAndLogs(client, bno)
				} else if len(*rpc) > 0 {
					sz, err = fetchBlockWithTxnAndLogsWithRPC(client, bno)
				} else {
					log.Fatalf("One of graphql or rpc URLs should be provided")
				}
				x.Check(err)

				times = append(times, time.Since(ts).Milliseconds())
				sizes = append(sizes, sz)
				atomic.AddUint64(&numBytes, uint64(sz))
				atomic.AddUint64(&numBlocks, 1)
			}
			mu.Lock()
			for _, t := range times {
				histDur.Update(t)
			}
			for _, sz := range sizes {
				histSz.Update(sz)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	fmt.Println("-----------------------")
	fmt.Printf("Latency in milliseconds")
	fmt.Println(histDur.String())

	fmt.Println("-----------------------")
	fmt.Printf("Resp size in bytes")
	fmt.Println(histSz.String())

	time.Sleep(2 * time.Second)
	fmt.Println("DONE")
}
