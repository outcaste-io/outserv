package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
)

var (
	url      = flag.String("url", "", "Outserv GraphQL endpoint")
	gor      = flag.Int("j", 128, "Num Goroutines to use")
	minBlock = flag.Int64("min", 6000000, "Min block number")
	maxBlock = flag.Int64("max", 14500000, "Max block number")
	dur      = flag.Duration("dur", time.Minute, "How long to run the benchmark")
	all      = flag.Bool("all", false, "Retrieve all fields.")
	sample   = flag.Int("sample", 1000, "Output query and response every N times")

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
	req, err := http.NewRequest("POST", *url, buf)
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
	if rand.Intn(1000) == 999 {
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

var numQueries uint64

func printQps() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	start := time.Now()
	rm := y.NewRateMonitor(300)
	for range ticker.C {
		numQ := atomic.LoadUint64(&numQueries)
		rm.Capture(numQ)

		dur := time.Since(start)
		fmt.Printf("Num Queries: %8d [ %6s @ %d qps ]\n",
			numQ, dur.Round(time.Second), rm.Rate())
	}
}

func main() {
	flag.Parse()

	rand.Seed(time.Now().UnixNano())
	end := time.Now().Add(*dur)

	go printQps()

	var mu sync.Mutex
	histDur := z.NewHistogramData(z.HistogramBounds(0, 20))
	histSz := z.NewHistogramData(z.HistogramBounds(0, 20))

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
				sz, err := fetchBlockWithTxnAndLogs(client, bno)
				x.Check(err)

				times = append(times, time.Since(ts).Milliseconds())
				sizes = append(sizes, sz)
				atomic.AddUint64(&numQueries, 1)
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
	fmt.Println("Latency in milliseconds")
	fmt.Println(histDur.String())

	fmt.Println("-----------------------")
	fmt.Println("Resp size in bytes")
	fmt.Println(histSz.String())

	fmt.Println("DONE")
}
