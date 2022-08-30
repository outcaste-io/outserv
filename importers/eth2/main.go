package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/outserv/x"
)

var url = flag.String("graphql", "", "Ethereum GraphQL endpoint")
var numGo = flag.Int("gor", 8, "Number of goroutines to use")
var blockRange = flag.Int64("block", 32, "Which block range to download in millions")

var curBlock, blockEnd int64
var numBlocks uint64
var numBytes uint64

const blockQuery = `{"query": "{block(number: %d) {baseFeePerGas,difficulty,extraData,gasLimit,gasUsed,hash,logsBloom,number,timestamp,miner{address},mixHash,nextBaseFeePerGas,nonce,number,ommers{hash},ommerHash,ommerCount,parent{hash},raw,rawHeader,receiptsRoot,stateRoot,timestamp,totalDifficulty,transactionsRoot,transactionCount,transactions{createdContract{address},cumulativeGasUsed,effectiveGasPrice,effectiveTip,from{address},gas,gasPrice,gasUsed,hash,index,inputData,maxFeePerGas,maxPriorityFeePerGas,to{address},nonce,r,raw,rawReceipt,s,status,type,v,value,logs{data,index,topics,account{address}}}}}"}`

type B struct {
	Block json.RawMessage `json:"block"`
}
type D struct {
	Data B `json:"data"`
}

func processBlocks(processId int, wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.Create(filepath.Join(
		dirName, fmt.Sprintf("eth-%02d.json.gz", processId)))
	x.Check(err)
	fmt.Printf("Created file: %s\n", f.Name())

	bw := bufio.NewWriterSize(f, 16<<20)
	gw, err := gzip.NewWriterLevel(bw, gzip.BestCompression)
	x.Check(err)

	defer func() {
		x.Check(gw.Flush())
		x.Check(bw.Flush())
		x.Check(f.Sync())
		x.Check(f.Close())
	}()

	for {
		blockNum := atomic.AddInt64(&curBlock, 1)
		if blockNum > blockEnd {
			return
		}
		q := fmt.Sprintf(blockQuery, blockNum)
		resp, err := http.Post(*url, "application/json", bytes.NewBufferString(q))
		x.Check(err)
		out, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		x.Check(err)

		var d D
		x.Check(json.Unmarshal(out, &d))
		if len(d.Data.Block) == 0 {
			fmt.Printf("Response for block: %d is empty: %s\n", blockNum, out)
			log.Fatal()
		}

		n, err := gw.Write(d.Data.Block)
		x.Check(err)

		atomic.AddUint64(&numBytes, uint64(n))
		atomic.AddUint64(&numBlocks, 1)
	}
}

func printQps() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	start := time.Now()
	rm := y.NewRateMonitor(300)
	bytesm := y.NewRateMonitor(300)
	for range ticker.C {
		numQ := atomic.LoadUint64(&numBlocks)
		numBytes := atomic.LoadUint64(&numBytes)
		rm.Capture(numQ)
		bytesm.Capture(numBytes / 1e6)

		dur := time.Since(start)
		fmt.Printf("Cur Block: %8d | Num Bytes: %s [ %6s @ %d bps | %d MBps ]\n",
			atomic.LoadInt64(&curBlock), humanize.IBytes(numBytes),
			dur.Round(time.Second), rm.Rate(), bytesm.Rate())
	}
}

var dirName string

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	var err error
	dirName, err = ioutil.TempDir(".", fmt.Sprintf("%d-", *blockRange))
	x.Check(err)

	go printQps()

	curBlock = *blockRange - 1
	blockEnd = curBlock + 1000000 // Included.
	var wg sync.WaitGroup
	for i := 0; i < *numGo; i++ {
		wg.Add(1)
		go processBlocks(i, &wg)
	}
	wg.Wait()
}
