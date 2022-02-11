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
var batchSize = flag.Int("batch", 100, "Num txns in one batch")

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

type WResp struct {
	DataResp `json:"data"`
}

var start = time.Now()
var sent int64

var mq = `
mutation($Post: [AddTxnInput!]!) {
	addTxn(input: $Post, upsert: true) {
		numUids
	}
}
`

func sendToGraphQL(b *Batch) error {
	q := GQL{
		Query:     mq,
		Variables: *b,
	}
	data, err := json.Marshal(q)
	if err != nil {
		log.Fatal(err)
	}

	// fmt.Printf("%s\n", data)

	// TODO: Check that the schema is correctly set.
	resp, err := http.Post("http://localhost:8080/graphql", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return errors.Wrapf(err, "while posting request")
	}
	out, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	var wr WResp
	if err := json.Unmarshal(out, &wr); err != nil {
		return errors.Wrapf(err, "response: %s\n", out)
	}
	s := atomic.AddInt64(&sent, 1)
	dur := time.Since(start)
	fmt.Printf("[ %04d at block: %06d ] [ %s @ %6.1f /s ] Batch added numUids: %03d\n", s,
		b.Txns[len(b.Txns)-1].Block, dur.Round(time.Second),
		float64(s)*float64(*batchSize)/dur.Seconds(),
		wr.Resp.NumUids)
	return nil
}

func processQueue(ch chan *Batch, wg *sync.WaitGroup) {
	defer wg.Done()
	for b := range ch {
		if err := sendToGraphQL(b); err != nil {
			log.Fatalf("Unable to send request: %v\n", err)
		}
	}
}

func main() {
	flag.Parse()

	client, err := ethclient.Dial(*path)
	if err != nil {
		log.Fatal(err)
	}

	chainID, err := client.NetworkID(context.Background())
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

	var wg sync.WaitGroup
	ch := make(chan *Batch, 10)
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go processQueue(ch, &wg)
	}

	var count int

	batch := &Batch{}
	defer func() {
		if len(batch.Txns) > 0 {
			ch <- batch
		}
		close(ch)
		wg.Wait()
	}()
	// Blocks before 46K don't seem to have any transactions.
	for i := int64(46000); i < header.Number.Int64(); i++ {
		// if i%1000 == 0 {
		// 	fmt.Printf("Block : %08d\n", i)
		// }
		if i > 1e6 {
			fmt.Println("Exiting...")
			return
		}
		blockNumber := big.NewInt(i)
		block, err := client.BlockByNumber(context.Background(), blockNumber)
		if err != nil {
			log.Fatal(err)
		}
		if len(block.Transactions()) == 0 {
			continue
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

			if len(batch.Txns) >= *batchSize {
				ch <- batch
				batch = &Batch{}
				count++
				if count > 1e6 {
					return
				}
			}

			// fmt.Println("hex", tx.Hash().Hex()) // 0x5d49fcaa394c97ec8a9c3e7bd9e8388d420fb050a52083ca52ff24b3b65bc9c2
			// tx.Hash()
			// fmt.Println(tx.Value().String())    // 10000000000000000
			// fmt.Println(tx.Gas())               // 105000
			// fmt.Println(tx.GasPrice().Uint64()) // 102000000000
			// fmt.Println(tx.Nonce())             // 110644
			// fmt.Println(tx.Data())              // []
			// fmt.Println(tx.To().Hex())          // 0x55fE59D8Ad77035154dDd0AD0388D09Dd4047A8e

			// receipt, err := client.TransactionReceipt(context.Background(), tx.Hash())
			// if err != nil {
			// 	fmt.Printf("txn: %x couldn't query receipt. Error: %v\n", tx.Hash(), err)
			// 	continue
			// }

			// fmt.Println("receipt:", receipt.Status) // 1
			// fmt.Println("logs:", receipt.Logs)      // ...
		}
	}
}
