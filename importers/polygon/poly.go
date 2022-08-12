package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var url = flag.String("url", "", "Bor URL address")
var dst = flag.String("dst", "", "Outserv GraphQL endpoint, or a directory")
var start = flag.Int64("start", 1, "Starting Block Number")
var end = flag.Int64("end", 0, "If set, stop at this Block Number, including it")
var gor = flag.Int("j", 4, "Num Goroutines to use")

func uid(writer io.Writer, typ, id string) string {
	if writer == nil {
		return ""
	}
	return fmt.Sprintf("_:%s.%s", typ, id)
}

func fetchBlock(writer io.Writer, blockNumber int64) (*BlockParsed, error) {
	fmt.Printf("Processing block: %d\n", blockNumber)
	q := fmt.Sprintf(`{
	"jsonrpc": "2.0",
	"method": "eth_getBlockByNumber",
	"params": ["%#x",true],
	"id": 1}`, blockNumber)

	resp, err := http.Post(*url, "application/json", strings.NewReader(q))
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to fetch block: %d", blockNumber)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read response")
	}

	type T struct {
		Result BlockRPC `json:"result"`
	}
	var t T
	Check(json.Unmarshal(data, &t))

	var out BlockParsed
	out.Block = t.Result.Block
	out.Uid = uid(writer, "Block", out.Hash)
	if len(t.Result.Miner) > 0 {
		out.Miner = Account{
			Uid:     uid(writer, "Account", t.Result.Miner),
			Address: t.Result.Miner,
		}
	}
	for _, t := range t.Result.Transactions {
		var outTxn TransactionParsed
		outTxn.Transaction = t.Transaction
		outTxn.From = Account{
			Uid:     uid(writer, "Account", t.From),
			Address: t.From,
		}
		outTxn.To = Account{
			Uid:     uid(writer, "Account", t.To),
			Address: t.To,
		}
		outTxn.Uid = uid(writer, "Transaction", outTxn.Hash)
		out.Transactions = append(out.Transactions, outTxn)
	}
	return &out, nil
}

func processBlock(gid int, wg *sync.WaitGroup) {
	defer wg.Done()

	var writer io.Writer
	fi, err := os.Stat(*dst)
	if err != nil {
		// This must be GraphQL endpoint.
	} else if fi.IsDir() {
		f, err := os.Create(filepath.Join(*dst, fmt.Sprintf("%02d.json.gz", gid)))
		Check(err)
		gw := gzip.NewWriter(f)
		writer = gw
		defer func() {
			gw.Flush()
			gw.Close()
			f.Sync()
			f.Close()
		}()
	}

	for num := range blockCh {
		block, err := fetchBlock(writer, num)
		if err != nil {
			fmt.Printf("Unable to parse block: %d\n", num)
			Check(err)
		}
		if writer != nil {
			data, err := json.Marshal(block)
			Check(err)
			_, err = writer.Write(data)
			Check(err)
		} else {
			// Send to Outserv directly
		}
	}
}

func Check(err error) {
	if err != nil {
		log.Fatalf("Got error: %v", err)
	}
}

func latestBlock() (int64, error) {
	q := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	resp, err := http.Post(*url, "application/json", strings.NewReader(q))
	if err != nil {
		return 0, errors.Wrapf(err, "while querying for latest block")
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.Wrapf(err, "Unable to read response")
	}
	type T struct {
		Result string `json:"result"`
	}
	var t T
	if err := json.Unmarshal(data, &t); err != nil {
		return 0, errors.Wrapf(err, "while unmarshal")
	}
	return strconv.ParseInt(t.Result, 0, 64)
}

var blockCh = make(chan int64, 16)

func main() {
	flag.Parse()

	bend, err := latestBlock()
	Check(err)
	fmt.Printf("Latest block: %d\n", bend)

	wg := &sync.WaitGroup{}
	for i := 0; i < *gor; i++ {
		wg.Add(1)
		go processBlock(i, wg)
	}

	curBlock := *start
	for {
		if *end != 0 {
			if curBlock > *end {
				break
			}
		}

		if curBlock <= bend {
			blockCh <- curBlock
			curBlock++
		} else {
			time.Sleep(time.Second)
			bend, err = latestBlock()
			Check(err)
		}
	}
	close(blockCh)
	wg.Wait()
}
