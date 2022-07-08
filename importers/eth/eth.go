// Copyright 2022 Outcaste LLC. Licensed under the Apache License v2.0.

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
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
)

var path = flag.String("geth", "", "Geth IPC path")
var graphql = flag.String("gql", "http://localhost:8080", "GraphQL endpoint")
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
	Hash string
}
type Txn struct {
	Hash  string
	Value int64
	Block int64
	To    Account
	From  Account
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
	Wg   sync.WaitGroup
	Id   int64
	txns []Txn
}

func (b *Block) Fill() {
	defer b.Wg.Done()
	blockNumber := big.NewInt(b.Id)
	block, err := client.BlockByNumber(context.Background(), blockNumber)
	check(err)
	for _, tx := range block.Transactions() {
		var to, from Account
		if msg, err := tx.AsMessage(types.NewEIP155Signer(chainID), nil); err == nil {
			from.Hash = msg.From().Hex()
		}

		if tx.To() != nil {
			to.Hash = tx.To().Hex()
		}
		txn := Txn{
			Hash:  tx.Hash().Hex(),
			Value: tx.Value().Int64(),
			Block: blockNumber.Int64(),
			To:    to,
			From:  from,
		}
		if txn.Value == 0 || len(txn.To.Hash) == 0 || len(txn.From.Hash) == 0 {
			continue
		}
		// Only include txns from certain accounts.
		include := true
		// if farm.Fingerprint64([]byte(txn.To.Hash))%1000 == 0 {
		// 	include = true
		// }
		// if farm.Fingerprint64([]byte(txn.From.Hash))%1000 == 0 {
		// 	include = true
		// }
		if include {
			b.txns = append(b.txns, txn)
		}
	}
}

func (b *Block) Txns() []Txn {
	b.Wg.Wait()
	return b.txns
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
		b := &Block{Id: blockId}
		b.Wg.Add(1)
		go b.Fill()
		c.blockCh <- b
	}
}

func (c *Chain) processTxns(wg *sync.WaitGroup) {
	defer wg.Done()

	var txns []Txn
	sendTxns := func(txns []Txn) error {
		if len(txns) == 0 {
			return nil
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

var client *ethclient.Client
var chainID *big.Int

func main() {
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	var err error
	client, err = ethclient.Dial(*path)
	if err != nil {
		log.Fatal(err)
	}

	chainID, err = client.NetworkID(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	header, err := client.HeaderByNumber(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Latest: %08d\n", header.Number)
	fmt.Printf("Using %d goroutines.\n", *numGo)

	chain := Chain{
		blockCh: make(chan *Block, 16),
	}
	var wg sync.WaitGroup
	for i := 0; i < *numGo; i++ {
		wg.Add(1)
		go chain.processTxns(&wg)
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
