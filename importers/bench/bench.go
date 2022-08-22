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

var url = flag.String("url", "", "Outserv GraphQL endpoint")
var gor = flag.Int("j", 32, "Num Goroutines to use")
var minBlock = flag.Int64("min", 6000000, "Min block number")
var maxBlock = flag.Int64("max", 14500000, "Max block number")
var dur = flag.Duration("dur", time.Minute, "How long to run the benchmark")

var blockQuery = `
{
	queryBlock(filter: {number: {eq: "%#x"}}) {
		number
		hash
		baseFeePerGas
		difficulty
		extraData
		gasLimit
		transactions {
			hash
			blockNumber
			logs {
				address
				topics
				data
				logIndex
			}
		}
	}
}
`

func fetchBlockWithTxnAndLogs(blockNum int64) (int64, error) {
	q := fmt.Sprintf(blockQuery, blockNum)
	// fmt.Printf("Query: %s\n", q)

	buf := bytes.NewBufferString(q)
	resp, err := http.Post(*url, "application/graphql", buf)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	// fmt.Printf("Resp:\n%s\n Query:\n%s\n", data, q)
	// os.Exit(1)
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

			var times []int64
			var sizes []int64
			for i := int64(0); ; i++ {
				ts := time.Now()
				if ts.After(end) {
					break
				}
				bno := rand.Int63n(N) + *minBlock
				sz, err := fetchBlockWithTxnAndLogs(bno)
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
