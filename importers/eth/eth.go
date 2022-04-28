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
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/outcaste-io/badger/v3/y"
	"github.com/pkg/errors"
)

var path = flag.String("geth", "", "Geth IPC path")
var graphql = flag.String("gql", "http://localhost:8080", "GraphQL endpoint")
var dryRun = flag.Bool("dry", false, "If true, don't send txns to GraphQL endpoint")
var numGo = flag.Int("gor", 2, "Number of goroutines to use")
var startBlock = flag.Int64("start", 50000, "Start at block")

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
		b.txns = append(b.txns, txn)
	}
}

func (b *Block) Txns() []Txn {
	b.Wg.Wait()
	return b.txns
}

type Chain struct {
	blockId      int64
	numProcessed uint64
	blockCh      chan *Block
}

func (c *Chain) BlockingFill() {
	defer close(c.blockCh)

	c.blockId = *startBlock - 1
	for {
		blockId := atomic.AddInt64(&c.blockId, 1)
		if blockId >= 14e6 {
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
			txns = txns[:0]
		}
		atomic.AddUint64(&c.numProcessed, 1)
	}
	check(sendTxns(txns))
}

func (c *Chain) printMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	rm := y.NewRateMonitor(300)
	start := time.Now()
	for range ticker.C {
		maxBlockId := atomic.LoadInt64(&c.blockId)
		num := atomic.LoadUint64(&c.numProcessed)
		rm.Capture(num)

		dur := time.Since(start)
		fmt.Printf("BlockId: %8d Processed: %5d [ %6s @ %5d blocks/min ]\n",
			maxBlockId, num,
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
