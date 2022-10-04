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

	"github.com/docker/docker/pkg/random"
	"github.com/dustin/go-humanize"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/outserv/x"
	"github.com/schollz/progressbar/v3"
)

var url = flag.String("graphql", "", "Ethereum GraphQL endpoint")
var numGo = flag.Int("gor", 8, "Number of goroutines to use")
var blockRange = flag.Int64("block", 32, "Which block range to download in millions")
var live = flag.Bool("live", false, "Polling geth client to catch up to the latest block and continuously keep updating")

var bpf = flag.Int("bpf", 1000, "Batch Save: Blocks per file in output .json. Defaults to 1000")

var GethRPCEndpoint = flag.String("geth", "http://localhost:8545", "Geth RPC endpoint. Defaults to http://localhost:8545")
var TargetApi = flag.String("target", "http://localhost:1248", "Target API endpoint. Defaults to http://localhost:1248")

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

		fmt.Print("Running live update mode ... \n")
		fmt.Printf("Target Api : %s\n", *TargetApi)
		fmt.Printf("Geth RPC Endpoint : %s\n", *GethRPCEndpoint)

		// fetch latest block from stream API
		resp, err := http.Get(fmt.Sprintf("%s/block/latest", *TargetApi))
		x.Check(err)
		out, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		x.Check(err)
		if len(out) == 0 {
			log.Fatal("latest block is empty")
		}

		// Cannot use BlockOut type here as int fields from graphql are stored as hex strings in stream
		var latestBlock map[string]interface{}
		x.Check(json.Unmarshal(out, &latestBlock))
		tmp, err := strconv.ParseInt(latestBlock["number"].(string), 0, 64)
		x.Check(err)
		fmt.Println("Latest Block on Stream =", latestBlock["number"], " ", tmp, "\tHash=", latestBlock["hash"])

		// Open/Create marker file
		mrkF, err := os.OpenFile("./marker.json", os.O_RDWR|os.O_CREATE, 0644)
		x.Check(err)

		defer func() {
			mrkF.Sync()
			mrkF.Close()
		}()

		byteVals, err := ioutil.ReadAll(mrkF)
		x.Check(err)

		latestBlockNumber, err := strconv.ParseInt(latestBlock["number"].(string), 0, 64)
		x.Check(err)

		var marker Marker
		if len(byteVals) == 0 {
			// marker file is empty, init the marker file
			fmt.Println("Marker file not found. creating new marker file ...")

			marker = Marker{
				ProcessedBlock:      latestBlockNumber,
				ProcessedBlockHash:  latestBlock["hash"].(string),
				Timestamp:           time.Now().UTC().String(),
				LatestGethBlock:     -1,
				LatestGethBlockHash: "",
			}

		} else {
			fmt.Println("Marker file found. reading marker ...")
			x.Check(json.Unmarshal(byteVals, &marker))

			// check the latest block on the [stream]/latest and update marker
			if marker.ProcessedBlock < latestBlockNumber {
				fmt.Printf("---xxx---\nINFO: marker previously used on %s was at block:%d but new latest block on streams is %d , updating marker to start from new latestBlock. \n---xxx---\n", marker.Timestamp, marker.ProcessedBlock, latestBlockNumber)
				marker.ProcessedBlock = latestBlockNumber
				marker.ProcessedBlockHash = latestBlock["hash"].(string)
				marker.Timestamp = time.Now().UTC().String()

			}

		}

		// RPC get latest block on gETH
		latestGethRes := JsonRpcEth("eth_blockNumber", "[]", 1)
		var latestGethBlockNumberHex string
		x.Check(json.Unmarshal(latestGethRes.Result, &latestGethBlockNumberHex))
		latestGethBlockNumber, err := strconv.ParseInt(latestGethBlockNumberHex, 0, 64)
		fmt.Println("Latest geth Block : ", latestGethBlockNumberHex, " ", latestGethBlockNumber)
		x.Check(err)

		marker.LatestGethBlock = latestGethBlockNumber
		marker.LatestGethBlockHash = "TODO"
		marker.Timestamp = time.Now().UTC().String()

		// write to file
		if mrkData, err := json.Marshal(marker); err == nil {
			x.Check(ioutil.WriteFile("./marker.json", mrkData, 0644))
		} else {
			log.Fatal(err)
		}

		fmt.Printf("Marker :\n Latest Stream Block: %d \n Latest geth block: %d\n", marker.ProcessedBlock, marker.LatestGethBlock)

		bar := progressbar.Default(marker.LatestGethBlock - marker.ProcessedBlock)
		var blocksBuffer []BlockOut

		// Start catching up to geth
		for blockNum := marker.ProcessedBlock + 1; blockNum <= marker.LatestGethBlock; blockNum++ {
			bar.Add(1)
			fmt.Printf("\nProcessing Block Number : %d 0x%x\n", blockNum, blockNum)
			// RPC
			var blkOut = new(BlockOut)

			// read data from geth json-rpc
			RPCFetchGethBlock(blockNum, blkOut)

			blocksBuffer = append(blocksBuffer, *blkOut)

			fmt.Printf("================\n")
			
			// TODO: for dev;write to file , replace later with network call to /write
			if len(blocksBuffer) == *bpf {
				fmt.Printf("\n Reached checkpoint of %d blocks at %d, writing to file ... \n", *bpf, blockNum)
				lo, err := strconv.ParseInt(blocksBuffer[0].Number, 0, 64)
				x.Check(err)
				hi, err := strconv.ParseInt(blocksBuffer[len(blocksBuffer)-1].Number, 0, 64)
				x.Check(err)
				bo, err := json.Marshal(blocksBuffer)
				x.Check(err)
				x.Check(ioutil.WriteFile(fmt.Sprintf("./tmp/%d-%d.json", lo, hi), bo, 0644))

				// reset buffer
				blocksBuffer = []BlockOut{}

				// update marker
				marker.ProcessedBlock = blockNum
				marker.Timestamp = time.Now().UTC().String()
				if mrkData, err := json.Marshal(marker); err == nil {
					x.Check(ioutil.WriteFile("./marker.json", mrkData, 0644))
				} else {
					log.Fatal(err)
				}
				fmt.Printf("\nSaved \"%d-%d.json\" successfully.\n", lo, hi)
			}

		}
	}

}

/*
	generic func to make rpc calls to eth
	- method : string - name of rpc method to call. Ex: eth_getBlockByNumber
	- params : string - must be stringified array of params
	- id : int64 - id of the rpc request
*/
func JsonRpcEth(method string, params string, id int64, verbose ...bool) GethRPCResponse {
	q := fmt.Sprintf(`{"jsonrpc":"2.0", "method":"%s", "params": %s, "id":%d}`, method, params, random.Rand.Int())
	if len(verbose) > 0 && verbose[0] {
		fmt.Println("RPC Start : ", q)
	}
	resp, err := http.Post(*GethRPCEndpoint, "application/json", bytes.NewBufferString(q))
	x.Check(err)
	if resp.StatusCode != 200 {
		fmt.Printf("%s(%d) : failed with response status \" %s \"from geth node. \n", method, id, resp.Status)
		log.Fatal("Non 200 Status Code")
	}

	out, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	x.Check(err)
	if len(out) == 0 {
		fmt.Printf("%s(%d) : Failed! Empty response body! \n", method, id)
		log.Fatal()
	}
	var res GethRPCResponse
	x.Check(json.Unmarshal(out, &res))

	if len(verbose) > 0 && verbose[0] {
		fmt.Printf("RPC done. Res size : %d bytes \n %s", len(out), string(out))
	}

	return res
}

func RPCFetchGethBlock(blockNumber int64, blkOut *BlockOut) {
	// read block data from json-rpc endpoint
	d := JsonRpcEth("eth_getBlockByNumber", fmt.Sprintf(`["%s",true]`, "0x"+strconv.FormatInt(blockNumber, 16)), blockNumber)
	if d.Result == nil {
		fmt.Printf("RPCFetchGethBlock: response for block: %d is empty: %+v\n", blockNumber, d)
		log.Fatal()
	}

	// create an intermediate struct to hold the data of BlockIn
	blkIn := new(BlockIn)
	x.Check(json.Unmarshal(d.Result, &blkIn))

	// parse block transactions
	fmt.Printf("blockNumber : %d , processing %d txns\n", blockNumber, len(blkIn.Transactions))
	txBar := progressbar.Default(int64(len(blkIn.Transactions)))
	processedTxns := make(map[string]bool)

	for _, tx := range blkIn.Transactions {
		txBar.Add(1)

		// check for duplicate transaction hash
		if processedTxns[tx.BlockNumber] {
			fmt.Println("Duplicate txn found! ", tx.Hash, " in block ", tx.BlockNumber, " is already present! Txn list :", processedTxns)
			log.Fatal()
		} else {
			processedTxns[tx.Hash] = true
		}

		data, err := json.Marshal(tx)
		x.Check(err)
		var txn TransactionIn
		x.Check(json.Unmarshal(data, &txn))

		// RPC get transaction logs
		eth_getTransactionReceipt := JsonRpcEth("eth_getTransactionReceipt", fmt.Sprintf(`["%s"]`, txn.Hash), int64(random.Rand.Int()))
		//  missing key root,contractAddress is received in above RPC res but not unpacked?
		// this can be because contractAddress can be null in which case it wont be unpacked, root is pre-byzantine
		x.Check(json.Unmarshal(eth_getTransactionReceipt.Result, tx))

		// RPC getTransaction by hash to get To,From Accounts
		eth_getTransactionByHash := JsonRpcEth("eth_getTransactionByHash", fmt.Sprintf(`["%s"]`, txn.Hash), blockNumber)
		var fullTxn TransactionByHashResponse
		x.Check(json.Unmarshal(eth_getTransactionByHash.Result, &fullTxn))

		// Fill in the fields which won't be present.
		blkOut.Transactions = append(blkOut.Transactions, &TransactionOut{
			Transaction: *tx,
			Type:        "Transaction",
			Fee:         txn.GasUsed, // Note: This field is not used in the current version of the indexer.
			From: &Account{
				Type:    "Account",
				Address: fullTxn.From,
			},
			To: &Account{
				Type:    "Account",
				Address: fullTxn.To,
			},
		})

	}

	// Add Custom Field Values
	// workaround for : blkOut.Block = blkIn.BlockInInner
	bi, err := json.Marshal(blkIn.BlockInInner)
	x.Check(err)
	err = json.Unmarshal(bi, &blkOut.Block)

	blkOut.Type = "Block"
	blkOut.Miner = &Account{
		Type:    "Account",
		Address: blkIn.Miner,
	}

	// Parse Ommers
	var idx int
	for idx = range blkIn.Uncles {

		// RPC get uncle block
		eth_getUncleByBlockHashAndIndex := JsonRpcEth("eth_getUncleByBlockHashAndIndex", fmt.Sprintf(`["%s","%s"]`, blkIn.Hash, "0x"+strconv.FormatInt(int64(idx), 16)), int64(random.Rand.Int()))

		data, err := json.Marshal(eth_getUncleByBlockHashAndIndex.Result)
		x.Check(err)

		var uin BlockIn
		x.Check(json.Unmarshal(data, &uin))
		var uout BlockOut

		// workaround:  uout.Block = uin.Block
		bi, err := json.Marshal(uin.BlockInInner)
		x.Check(err)
		err = json.Unmarshal(bi, &uout.Block)

		if len(uin.Miner) > 0 {
			uout.Miner = &Account{Address: uin.Miner}
		}
		blkOut.Ommers = append(blkOut.Ommers, uout)
	}

	blkOut.OmmerCount = fmt.Sprintf("%d", len(blkIn.Uncles))

}

func CompressBlockOut(data json.RawMessage) *bytes.Buffer {
	var b bytes.Buffer

	gw, err := gzip.NewWriterLevel(&b, gzip.BestCompression)
	x.Check(err)
	gw.Write(data)

	gw.Flush()
	gw.Close()
	return &b
}

func DecompressBlockOut(checkB bytes.Buffer) string {
	gr, err := gzip.NewReader(&checkB)
	x.Check(err)
	d, err := ioutil.ReadAll(gr)
	x.Check(err)
	return string(d)
}
