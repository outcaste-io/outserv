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
	"github.com/pkg/errors"
)

var path = flag.String("geth", "", "Geth IPC path")
var graphql = flag.String("gql", "", "GraphQL endpoint")
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
	numProcessed int64
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
		if len(txns) >= 1000 {
			check(sendTxns(txns))
			txns = txns[:0]
		}
		atomic.AddInt64(&c.numProcessed, 1)
	}
	check(sendTxns(txns))
}

func (c *Chain) printMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	start := time.Now()
	for range ticker.C {
		maxBlockId := atomic.LoadInt64(&c.blockId)
		num := atomic.LoadInt64(&c.numProcessed)
		dur := time.Since(start)
		fmt.Printf("BlockId: %d Processed: %d [ %d @ %6.1f blocks/min ]\n",
			maxBlockId, num,
			dur.Round(time.Second), float64(num)/dur.Minutes())
	}
}
func accountsFrom(out map[string]Account, blockId int64) {
	blockNumber := big.NewInt(blockId)
	block, err := client.BlockByNumber(context.Background(), blockNumber)
	if err != nil {
		log.Fatal(err)
	}

	for _, tx := range block.Transactions() {
		var to, from Account
		if msg, err := tx.AsMessage(types.NewEIP155Signer(chainID), nil); err == nil {
			from.Hash = msg.From().Hex()
		}
		if _, has := out[from.Hash]; !has {
			out[from.Hash] = from
		}

		if tx.To() != nil {
			to.Hash = tx.To().Hex()
			if _, has := out[to.Hash]; !has {
				out[to.Hash] = to
			}
		}
	}
}

func sendRequest(data []byte) error {
	if *dryRun {
		return nil
	}
	// TODO: Check that the schema is correctly set.
	var wr WResp
	resp, err := http.Post("http://localhost:8080/graphql", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return errors.Wrapf(err, "while posting request")
	}
	out, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	// fmt.Printf("out: %s\n", out)

	if err := json.Unmarshal(out, &wr); err != nil {
		return errors.Wrapf(err, "response: %s\n", out)
	}
	// TODO: Remove this once we remove txns from Outserv.
	for _, werr := range wr.Errors {
		if len(werr.Message) > 0 {
			return fmt.Errorf("Got error from GraphQL: %s\n", werr.Message)
		}
	}

	return nil
}

const numBlocks int64 = 64

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

	// account := common.HexToAddress("0xad0e5c205e8c1f6dbeae4cf8b69b693390c13617")
	// balance, err := client.BalanceAt(context.Background(), account, nil)
	// if err != nil {
	// 	log.Fatalf("balance without block: %v", err)
	// }
	// fmt.Println("bal", balance) // 25893180161173005034

	// blockNumber := big.NewInt(5532993)
	// balanceAt, err := client.BalanceAt(context.Background(), account, blockNumber)
	// if err != nil {
	// 	log.Fatalf("balance at error: %v", err)
	// }
	// fmt.Println(balanceAt) // 25729324269165216042

	// fbalance := new(big.Float)
	// fbalance.SetString(balance.String())
	// ethValue := new(big.Float).Quo(fbalance, big.NewFloat(math.Pow10(18)))
	// fmt.Println("eth bal", ethValue) // 25.729324269165216041

	// pendingBalance, err := client.PendingBalanceAt(context.Background(), account)
	// fmt.Println("pending", pendingBalance) // 25729324269165216042

	// address := common.HexToAddress("0xad0e5c205e8c1f6dbeae4cf8b69b693390c13617")

	// fmt.Println(address.Hex())        // 0x71C7656EC7ab88b098defB751B7401B5f6d8976F
	// fmt.Println(address.Hash().Hex()) // 0x00000000000000000000000071c7656ec7ab88b098defb751b7401b5f6d8976f

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
