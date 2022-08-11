package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
)

var path = flag.String("rpc", "", "Polygon Bor JSON RPC path")
var graphql = flag.String("gql", "http://localhost:8080", "Outserv GraphQL endpoint")
var dryRun = flag.Bool("dry", false, "If true, don't send txns to GraphQL endpoint")
var numGo = flag.Int("gor", 1, "Number of goroutines to use")
var startBlock = flag.Int64("start", 1, "Start at block, including this one")
var blockId = flag.Int64("block", 0, "If set, only fetch this block")

var client *ethclient.Client
var chainID *big.Int
var writeUids bool
var gwei = big.NewInt(1e9)

func sendRequest(data []byte) error {
	if *dryRun {
		return nil
	}
	// TODO: Check that the schema is correctly set.
	var wr WResp
	resp, err := http.Post(fmt.Sprintf("%s/graphql", *graphql),
		"application/json", bytes.NewBuffer(data))
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

var blockMu = `
mutation($Blks: [AddBlockInput!]!) {
	addBlock(input: $Blks, upsert: true) {
		numUids
	}
}
`

type Batch struct {
	Blks []Block `json:"Blks,omitempty"`
}
type GQL struct {
	Query     string `json:"query"`
	Variables Batch  `json:"variables"`
}
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

type Chain struct {
	blockId   int64
	numBlocks uint64
	numTxns   uint64
	blockCh   chan *Block
}

func (c *Chain) printMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	rm := NewRateMonitor(300)
	start := time.Now()
	for range ticker.C {
		maxBlockId := atomic.LoadInt64(&c.blockId)
		num := atomic.LoadUint64(&c.numBlocks)
		rm.Capture(num)

		dur := time.Since(start)
		fmt.Printf("BlockId: %8d Processed: %5d blocks %5d txns [ %6s @ %5d blocks/min ]\n",
			maxBlockId, num, atomic.LoadUint64(&c.numTxns),
			dur.Round(time.Second), rm.Rate()*60.0)
	}
}

func getLatest() int64 {
	header, err := client.HeaderByNumber(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	return header.Number.Int64()
}

func (c *Chain) BlockingFill() {
	defer close(c.blockCh)

	if *blockId > 0 {
		b := &Block{Number: *blockId}
		b.wg.Add(1)
		go b.Fill()
		c.blockCh <- b
		return
	}

	latest := getLatest()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			num := getLatest()
			atomic.StoreInt64(&latest, num)
		}
	}()

	c.blockId = *startBlock - 1
	for {
		blockId := atomic.AddInt64(&c.blockId, 1)
		for {
			end := atomic.LoadInt64(&latest)
			if blockId <= end {
				break
			}
			time.Sleep(time.Second)
		}
		b := &Block{Number: blockId}
		b.wg.Add(1)
		go b.Fill()
		c.blockCh <- b
	}
}

func (c *Chain) processTxns(gid int, wg *sync.WaitGroup) {
	defer wg.Done()

	var writtenMB float64
	defer func() {
		fmt.Printf("Process %d wrote %.2f GBs of data\n", gid, writtenMB/(1<<10))
	}()

	sendBlks := func(blks []Block) error {
		if len(blks) == 0 {
			return nil
		}
		q := GQL{
			Query:     blockMu,
			Variables: Batch{Blks: blks},
		}
		data, err := json.Marshal(q)
		if err != nil {
			return err
		}
		fmt.Printf("data:\n%s\n", data)
		return sendRequest(data)
	}

	for block := range c.blockCh {
		block.Wait()
		if len(block.Transactions) == 0 {
			continue
		}
		Check(sendBlks([]Block{*block}))
		atomic.AddUint64(&c.numTxns, uint64(len(block.Transactions)))
		atomic.AddUint64(&c.numBlocks, 1)
	}
}

var contractAbi abi.ABI
var signer types.Signer

func main() {
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	fdata, err := ioutil.ReadFile("mrc20.abi")
	Check(err)

	contractAbi, err = abi.JSON(bytes.NewReader(fdata))
	Check(err)

	client, err = ethclient.Dial(*path)
	if err != nil {
		log.Fatal(err)
	}

	chainID, err = client.NetworkID(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	signer = types.LatestSignerForChainID(chainID)

	header, err := client.HeaderByNumber(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Chain ID: %d Latest: %08d\n", chainID.Uint64(), header.Number)

	chain := Chain{
		blockCh: make(chan *Block, 16),
	}

	var wg sync.WaitGroup
	for i := 0; i < *numGo; i++ {
		wg.Add(1)
		go chain.processTxns(i, &wg)
	}
	go chain.printMetrics()
	chain.BlockingFill()
	wg.Wait()
	fmt.Println("DONE")
}
