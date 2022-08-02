// Copyright 2022 Outcaste LLC. Licensed under the Apache License v2.0.

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
)

var path = flag.String("geth", "", "Geth IPC path or GraphQL Address."+
	" Recommendation is to use the GraphQL address for performance.")
var graphql = flag.String("gql", "http://localhost:8080", "Outserv GraphQL endpoint")
var outDir = flag.String("dir", "", "Output to dir as JSON.GZ files instead of sending to Outserv.")
var outIPC = flag.String("ipc", "", "Output IPC dir for loader to read directly from.")
var dryRun = flag.Bool("dry", false, "If true, don't send txns to GraphQL endpoint")
var numGo = flag.Int("gor", 4, "Number of goroutines to use")
var startBlock = flag.Int64("start", 14900000, "Start at block")

var txnMu = `
mutation($Txns: [AddTxnInput!]!) {
	addTxn(input: $Txns, upsert: true) {
		numUids
	}
}
`

type Batch struct {
	Txns []Txn `json:"Txns"`
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

var gwei = big.NewInt(1e9)

func (b *Block) Fill() {
	defer b.wg.Done()
	if isGraphQL {
		b.fastFillViaGraphQL()
	} else {
		b.slowFillViaClient()
	}
}

func (b *Block) Txns() []Txn {
	b.wg.Wait()
	return b.Transactions
}

type Chain struct {
	blockId   int64
	numBlocks uint64
	numTxns   uint64
	blockCh   chan *Block
}

func (c *Chain) BlockingFill() {
	defer close(c.blockCh)

	c.blockId = *startBlock - 1
	for {
		blockId := atomic.AddInt64(&c.blockId, 1)
		if blockId >= 15e6 {
			return
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

	var writer io.Writer
	if len(*outDir) > 0 {
		f, err := os.Create(filepath.Join(*outDir, fmt.Sprintf("%02d.json.gz", gid)))
		check(err)

		gw := gzip.NewWriter(f)
		writer = gw
		defer func() {
			gw.Flush()
			gw.Close()
			f.Sync()
			f.Close()
		}()
	} else if len(*outIPC) > 0 {
		path := filepath.Join(*outIPC, fmt.Sprintf("%02d.ipc", gid))
		fmt.Printf("Creating IPC: %s\n", path)
		check(syscall.Mkfifo(path, 0666))

		fd, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, os.ModeNamedPipe)
		check(err)
		defer fd.Close()

		// No need to do any buffering. It would just cause an
		// additional copy for no good reason.
		writer = fd
	}

	var txns []Txn
	sendTxns := func(txns []Txn) error {
		if len(txns) == 0 {
			return nil
		}
		if writer != nil {
			data, err := json.Marshal(txns)
			check(err)
			n, err := writer.Write(data)
			writtenMB += float64(n) / (1 << 20)
			return err
		}

		q := GQL{
			Query:     txnMu,
			Variables: Batch{Txns: txns},
		}
		data, err := json.Marshal(q)
		if err != nil {
			return err
		}
		return sendRequest(data)
	}

	for block := range c.blockCh {
		txns = append(txns, block.Txns()...)
		if len(txns) >= 100 {
			check(sendTxns(txns))
			atomic.AddUint64(&c.numTxns, uint64(len(txns)))
			txns = txns[:0]
		}
		atomic.AddUint64(&c.numBlocks, 1)
	}
	check(sendTxns(txns))
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

func check(err error) {
	if err != nil {
		log.Fatalf("Got error: %v", err)
	}
}

var isGraphQL bool
var client *ethclient.Client
var chainID *big.Int
var writeUids bool

func main() {
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	if strings.HasSuffix(*path, "/graphql") {
		isGraphQL = true
	} else {
		var err error
		client, err = ethclient.Dial(*path)
		if err != nil {
			log.Fatal(err)
		}
	}
	if len(*outDir) > 0 {
		fmt.Printf("Outputting JSON files to %q\n", *outDir)
		*graphql = ""
		writeUids = true
	} else if len(*outIPC) > 0 {
		fmt.Printf("Outputting to IPC files at %q\n", *outIPC)
		os.RemoveAll(*outIPC)
		os.MkdirAll(*outIPC, 0755)
		writeUids = true
	}

	if !isGraphQL {
		var err error
		chainID, err = client.NetworkID(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		header, err := client.HeaderByNumber(context.Background(), nil)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Latest: %08d\n", header.Number)
	}
	fmt.Printf("Using %d goroutines.\n", *numGo)

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
