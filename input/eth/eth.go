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
	"strings"
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
var numGo = flag.Int("gor", 3, "Number of goroutines to use")

type Txn struct {
	Hash  string
	Value int64
	Block int64
	To    Account
	From  Account
}

type Account struct {
	Hash string
}

type Batch struct {
	Txns []Txn `json:"Post"`
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

var start = time.Now()
var sent int64
var numTxns int64

var mq = `
mutation($Post: [AddTxnInput!]!) {
	addTxn(input: $Post, upsert: true) {
		numUids
	}
}
`

func createBatchFrom(blockId int64) Batch {
	blockNumber := big.NewInt(blockId)
	block, err := client.BlockByNumber(context.Background(), blockNumber)
	if err != nil {
		log.Fatal(err)
	}

	// fmt.Println(block.Number().Uint64())     // 5671744
	// fmt.Println(block.Difficulty().Uint64()) // 3217000136609065
	// fmt.Println(block.Hash().Hex())          // 0x9e8751ebb5069389b855bba72d94902cc385042661498a415979b7b6ee9ba4b9
	// fmt.Println(len(block.Transactions()))   // 144

	// count, err := client.TransactionCount(context.Background(), block.Hash())
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("txn count: %d for block: %d\n", len(block.Transactions()), blockNumber)

	var batch Batch
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
		batch.Txns = append(batch.Txns, txn)
	}
	return batch
}

func processBlock(blockId int64) error {
	batch := createBatchFrom(blockId)
	if len(batch.Txns) == 0 {
		return nil
	}

	q := GQL{
		Query:     mq,
		Variables: batch,
	}
	data, err := json.Marshal(q)
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Check that the schema is correctly set.
	var wr WResp
	if !*dryRun {
	LOOP:
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
			if strings.Contains(werr.Message, "has been aborted") {
				// We need to retry.
				fmt.Printf("Retrying update. Error: %v\n", werr.Message)
				goto LOOP
			}
			if len(werr.Message) > 0 {
				return fmt.Errorf("Got error from GraphQL: %s\n", werr.Message)
			}
		}
	}
	s := atomic.AddInt64(&sent, 1)
	num := atomic.AddInt64(&numTxns, int64(len(batch.Txns)))
	dur := time.Since(start)
	fmt.Printf("[ %04d at block: %06d ] [ %s @ %6.1f /s ] Batch added numUids: %03d\n", s,
		blockId, dur.Round(time.Second), float64(num)/dur.Seconds(), wr.Data.Resp.NumUids)
	return nil
}

var curBlock int64 = 46000

func processQueue(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		blockId := atomic.AddInt64(&curBlock, 1)
		if blockId > 1e6 {
			return
		}
		if err := processBlock(blockId); err != nil {
			log.Fatalf("Unable to process Block: %v\n", err)
		}
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

	var wg sync.WaitGroup
	for i := 0; i < *numGo; i++ {
		wg.Add(1)
		go processQueue(&wg)
	}
	wg.Wait()
}
