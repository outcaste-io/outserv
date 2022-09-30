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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/outserv/x"
)

const (
	Fastx100EthApi  = "https://stream.0xfast.com/beta/eth-mainnet"
	GethRPCEndpoint = "http://localhost:8545"
)

var url = flag.String("graphql", "", "Ethereum GraphQL endpoint")
var numGo = flag.Int("gor", 8, "Number of goroutines to use")
var blockRange = flag.Int64("block", 32, "Which block range to download in millions")
var live = flag.Bool("live", false, "Polling geth client to catch up to the latest block and continuously keep updating")

var curBlock, blockEnd int64
var numBlocks uint64
var numBytes uint64

var dirName string

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

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	if !*live {
		// Bulk update mode
		var err error
		dirName, err = ioutil.TempDir(".", fmt.Sprintf("%d-", *blockRange))
		x.Check(err)

		go printQps()

		curBlock = (*blockRange * 1000000) - 1
		blockEnd = curBlock + 1000000 // Included.
		var wg sync.WaitGroup
		for i := 0; i < *numGo; i++ {
			wg.Add(1)
			go processBlocks(i, &wg)
		}
		wg.Wait()
	} else {
		/*
			Live update mode
			   x1. Retrieve marker of last block processed AKA Current starting point.
			      query [stream]/latest and get the latest block details and create a marker file + read it.

			      if marker file is not present:
					InitMarkerFile()

				  else: marker file
					{
						ProcessedBlock: int64       | initial val : [stream]/latest
						ProcessedBlockHash: string  | initial val : [stream]/latest
						LatestBlockHeight: int64    | from geth api
						LatestBlockHash: string     | from geth api
						ts: time.Time>string        | initial val : time.Now()
					}

			   x2. Req geth client to get latest block number and update marker file.
			   3. call processBlocks() : curBlock = ProcessedBlock + 1, blockEnd = LatestBlockHeight;
				  result: new blocks are downloaded and stored as json.gz file.
			   4. on complete update marker file with latest ProcessedBlock and ProcessedBlockHash.

			TODO: update stream to use marker file when running parseFile() or add /bulk-write
		*/

		fmt.Print("Live update mode is not implemented yet.\n")
		fmt.Printf("Using Fastx100 ETH : %s\n", Fastx100EthApi)

		// fetch latest block from stream API
		resp, err := http.Get(fmt.Sprintf("%s/latest", Fastx100EthApi))
		x.Check(err)
		out, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		x.Check(err)
		if len(out) == 0 {
			log.Fatal("latest block is empty")
		}

		var latestBlock BlockOut
		x.Check(json.Unmarshal(out, &latestBlock))

		// Open/Create marker file
		mrkF, err := os.OpenFile("./marker.json", os.O_RDWR|os.O_CREATE, 0644)
		x.Check(err)

		defer func() {
			mrkF.Sync()
			mrkF.Close()
		}()

		byteVals, err := ioutil.ReadAll(mrkF)
		x.Check(err)

		latestBlockNumber, err := strconv.ParseInt(latestBlock.Number, 0, 64)
		x.Check(err)

		var marker Marker
		if len(byteVals) == 0 {
			// marker file is empty, init the marker file
			fmt.Println("marker file not found. creating new marker file ...")

			marker = Marker{
				ProcessedBlock:      latestBlockNumber,
				ProcessedBlockHash:  latestBlock.Hash,
				Timestamp:           time.Now().UTC().String(),
				LatestGethBlock:     -1,
				LatestGethBlockHash: "",
			}

		} else {
			// marker file is not empty, read the marker file
			fmt.Println("marker file found. reading marker file ...")
			x.Check(json.Unmarshal(byteVals, &marker))

			// check the latest block on the [stream]/latest and update marker
			if marker.ProcessedBlock < latestBlockNumber {
				fmt.Printf("INFO: marker previously used on %s was at block:%d but new latest block on streams is %d , updating marker to start from new latestBlock. \n", marker.Timestamp, marker.ProcessedBlock, latestBlockNumber)

				marker.ProcessedBlock = latestBlockNumber
				marker.ProcessedBlockHash = latestBlock.Hash
				marker.Timestamp = time.Now().UTC().String()

			}

		}
		// fetch the latest block of the chain from geth client and update marker

		// get geth status

		// https://geth.ethereum.org/docs/rpc/graphql : Here’s how you’d get the latest block’s number
		const gethBlockQuery = `{ "query": "query { block { number,hash } }" }`
		const graphqlEndpoint = "http://localhost:8545/graphql"

		fmt.Println("Fetching geth latest block ...")
		resp, err = http.Post(graphqlEndpoint, "application/json", bytes.NewBufferString(gethBlockQuery))
		if err != nil {
			fmt.Println("graphql post req to geth node failed")
			log.Fatal(err)
		}
		out, err = ioutil.ReadAll(resp.Body)
		x.Check(err)
		resp.Body.Close()
		if len(out) == 0 {
			fmt.Println("failed to read response body from geth graphql. Query : gethBlockQuery")
			log.Fatal(err)
		}

		// update marker with latest block known to geth node

		/* sample res :
		{"data":{"block":{"number":15465208,
		"hash":"0x2c486895b9e47ca119baa0da3b576234460886aef92a31b01f7abd3786395f7d"}}}
		*/

		var gethBlockRes GethBockResponse
		x.Check(json.Unmarshal(out, &gethBlockRes))
		marker.LatestGethBlock = gethBlockRes.Data.Block.Number
		marker.LatestGethBlockHash = gethBlockRes.Data.Block.Hash
		marker.Timestamp = time.Now().UTC().String()

		if mrkData, err := json.Marshal(marker); err == nil {
			mrkF.WriteAt(mrkData, 0)
			x.Check(mrkF.Sync())
		} else {
			log.Fatal(err)
		}

		fmt.Printf("Marker :\n Latest Stream Block: %d \n Latest geth block: %d\n", marker.ProcessedBlock, marker.LatestGethBlock)

		for i := marker.ProcessedBlock + 1; i <= marker.LatestGethBlock; i++ {
			// Todo
		}
	}

}
