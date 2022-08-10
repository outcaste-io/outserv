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

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
)

var path = flag.String("rpc", "", "Polygon Bor JSON RPC path")
var graphql = flag.String("gql", "http://localhost:8080", "Outserv GraphQL endpoint")
var dryRun = flag.Bool("dry", false, "If true, don't send txns to GraphQL endpoint")
var numGo = flag.Int("gor", 4, "Number of goroutines to use")
var startBlock = flag.Int64("start", 0, "Start at block")

var client *ethclient.Client
var chainID *big.Int
var writeUids bool

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

type Chain struct {
	blockId   int64
	numBlocks uint64
	numTxns   uint64
	blockCh   chan *Block
}

func (c *Chain) processTxns(gid int, wg *sync.WaitGroup) {
	defer wg.Done()

	var writtenMB float64
	defer func() {
		fmt.Printf("Process %d wrote %.2f GBs of data\n", gid, writtenMB/(1<<10))
	}()

	var txns []Txn
	sendTxns := func(txns []Txn) error {
		if len(txns) == 0 {
			return nil
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
