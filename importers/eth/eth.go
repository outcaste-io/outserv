// Copyright 2022 Outcaste LLC. Licensed under the Apache License v2.0.

package main

import (
	"bufio"
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
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
)

var path = flag.String("geth", "", "Geth IPC path or GraphQL Address")
var graphql = flag.String("gql", "http://localhost:8080", "Outserv GraphQL endpoint")
var outDir = flag.String("dir", "", "Output to dir as JSON.GZ files instead of sending to Outserv.")
var outPipe = flag.String("socket", "", "Output socket dir for bulk loader to read from.")
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

type Account struct {
	Uid     string `json:"uid,omitempty"`
	Address string `json:"address"`
}
type Txn struct {
	Uid         string  `json:"uid,omitempty"`
	Hash        string  `json:"hash"`
	Value       int64   `json:"value"`
	Fee         int64   `json:"fee"`
	BlockNumber int64   `json:"blockNumber"`
	Timestamp   int64   `json:"timestamp"`
	Block       Block   `json:"block"`
	To          Account `json:"to"`
	From        Account `json:"from"`

	// The following fields are used by ETH. But, not part of Outserv's GraphQL Schema.
	ValueStr string `json:"value_str,omitempty"`
	GasUsed  int64  `json:"gasUsed,omitempty"`
	GasPrice string `json:"gasPrice,omitempty"`
}

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
type Block struct {
	wg           sync.WaitGroup
	Uid          string `json:"uid,omitempty"`
	Number       int64  `json:"number"`
	Timestamp    string `json:"timestamp,omitempty"`
	Transactions []Txn  `json:"transactions,omitempty"`
}

var gwei = big.NewInt(1e9)

func (b *Block) fillViaClient() {
	blockNumber := big.NewInt(b.Number)
	block, err := client.BlockByNumber(context.Background(), blockNumber)
	check(err)
	for _, tx := range block.Transactions() {
		receipt, err := client.TransactionReceipt(context.Background(), tx.Hash())
		check(err)
		gasUsed := new(big.Int).SetUint64(receipt.GasUsed)
		if receipt.Status != 1 {
			// Skip failed transactions.
			continue
		}

		var to, from Account
		if msg, err := tx.AsMessage(types.NewEIP155Signer(chainID), nil); err == nil {
			from.Address = msg.From().Hex()
		}

		if tx.To() != nil {
			to.Address = tx.To().Hex()
		}

		valGwei := new(big.Int).Div(tx.Value(), gwei)
		fee := new(big.Int).Mul(tx.GasPrice(), gasUsed)
		feeGwei := new(big.Int).Div(fee, gwei)
		txn := Txn{
			Hash:        tx.Hash().Hex(),
			Value:       valGwei.Int64(),
			Fee:         feeGwei.Int64(),
			BlockNumber: b.Number,
			Block:       Block{Number: b.Number},
			To:          to,
			From:        from,
		}
		if len(txn.To.Address) == 0 || len(txn.From.Address) == 0 {
			continue
		}
		b.Transactions = append(b.Transactions, txn)
	}
}

func (b *Block) fillViaGraphQL() {
	q := fmt.Sprintf(`
{
	block(number: %d) {
		number
		timestamp
		transactions {
			hash
			from { address }
			to { address }
			value_str: value
			gasPrice
			gasUsed
		}
	}
}`, b.Number)

	type httpReq struct {
		Query string
	}
	req := httpReq{Query: q}
	reqData, err := json.Marshal(req)
	check(err)

	buf := bytes.NewBuffer(reqData)
	resp, err := http.Post(*path, "application/graphql", buf)
	check(err)

	data, err := ioutil.ReadAll(resp.Body)
	check(err)
	check(resp.Body.Close())

	type eResp struct {
		Data struct {
			Block Block
		} `json:"data"`
	}
	var eresp eResp
	check(json.Unmarshal(data, &eresp))

	blk := eresp.Data.Block
	if blk.Number != b.Number {
		fmt.Printf("Invalid response from ETH. Expected: %d Got: %d\n", b.Number, blk.Number)
		os.Exit(1)
	}
	ts, err := strconv.ParseInt(blk.Timestamp, 0, 64)
	if err != nil {
		fmt.Printf("Unable to parse timestamp: %s\n", blk.Timestamp)
		ts = 0
	}
	for _, txn := range blk.Transactions {
		val, ok := new(big.Int).SetString(txn.ValueStr, 0)
		if !ok {
			val = new(big.Int).SetInt64(0)
		}
		txn.Value = new(big.Int).Div(val, gwei).Int64()

		if len(txn.To.Address) == 0 || len(txn.From.Address) == 0 {
			continue
		}
		txn.To.Uid = fmt.Sprintf("_:Account.%s", txn.To.Address)
		txn.From.Uid = fmt.Sprintf("_:Account.%s", txn.From.Address)

		price, ok := new(big.Int).SetString(txn.GasPrice, 0)
		if !ok {
			price = new(big.Int).SetInt64(0)
		}
		fee := new(big.Int).Mul(price, new(big.Int).SetInt64(txn.GasUsed))
		feeGwei := new(big.Int).Div(fee, gwei)
		txn.Fee = feeGwei.Int64()
		txn.BlockNumber = b.Number

		txn.Timestamp = ts
		txn.Block = Block{Number: b.Number, Uid: fmt.Sprintf("_:Block.%08d", b.Number)}
		txn.Uid = fmt.Sprintf("_:Txn.%s", txn.Hash)

		// Zero out the following fields, so they don't get marshalled when
		// sending to Outserv.
		txn.ValueStr = ""
		txn.GasUsed = 0
		txn.GasPrice = ""
		b.Transactions = append(b.Transactions, txn)
	}
}

func (b *Block) Fill() {
	defer b.wg.Done()
	if isGraphQL {
		b.fillViaGraphQL()
	} else {
		b.fillViaClient()
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
	} else if len(*outPipe) > 0 {
		path := filepath.Join(*outPipe, fmt.Sprintf("%02d.ipc", gid))
		fmt.Printf("Listening on path: %s\n", path)
		l, err := net.Listen("unix", path)
		check(err)
		fmt.Printf("Waiting for connection on path: %s\n", path)
		conn, err := l.Accept()
		check(err)
		fmt.Printf("Got connection for path: %s. Writing...\n", path)
		bw := bufio.NewWriterSize(conn, 1<<20)
		writer = bw
		defer func() {
			bw.Flush()
			conn.Close()
			l.Close()
		}()
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
		// fmt.Printf("----------> Txns %d. Sending...\n", len(txns))
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

type RateMonitor struct {
	start       time.Time
	lastSent    uint64
	lastCapture time.Time
	rates       []float64
	idx         int
}

func NewRateMonitor(numSamples int) *RateMonitor {
	return &RateMonitor{
		start: time.Now(),
		rates: make([]float64, numSamples),
	}
}

const minRate = 0.0001

// Capture captures the current number of sent bytes. This number should be monotonically
// increasing.
func (rm *RateMonitor) Capture(sent uint64) {
	diff := sent - rm.lastSent
	dur := time.Since(rm.lastCapture)
	rm.lastCapture, rm.lastSent = time.Now(), sent

	rate := float64(diff) / dur.Seconds()
	if rate < minRate {
		rate = minRate
	}
	rm.rates[rm.idx] = rate
	rm.idx = (rm.idx + 1) % len(rm.rates)
}

// Rate returns the average rate of transmission smoothed out by the number of samples.
func (rm *RateMonitor) Rate() uint64 {
	var total float64
	var den float64
	for _, r := range rm.rates {
		if r < minRate {
			// Ignore this. We always set minRate, so this is a zero.
			// Typically at the start of the rate monitor, we'd have zeros.
			continue
		}
		total += r
		den += 1.0
	}
	if den < minRate {
		return 0
	}
	return uint64(total / den)
}
