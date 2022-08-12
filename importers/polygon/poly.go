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
	"strings"
	"sync"

	"github.com/pkg/errors"
)

var url = flag.String("url", "", "Bor URL address")
var dst = flag.String("dst", "", "Outserv GraphQL endpoint, or a directory")

func uid(typ, id string) string {
	if writer == nil {
		return ""
	}
	return fmt.Sprintf("_:%s.%s", typ, id)
}

func fetchBlock(blockNumber int64) (*BlockParsed, error) {
	q := fmt.Sprintf(`{
	"jsonrpc": "2.0",
	"method": "eth_getBlockByNumber",
	"params": ["%#x",true],
	"id": 1}`, blockNumber)

	fmt.Printf("url: %s\n q: %s\n", *url, q)
	resp, err := http.Post(*url, "application/json", strings.NewReader(q))
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to fetch block: %d", blockNumber)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read response")
	}
	fmt.Printf("Got resp:\n%s\n---\n", data)

	type T struct {
		Result BlockRPC `json:"result"`
	}
	var t T
	Check(json.Unmarshal(data, &t))
	fmt.Printf("Parsed block: %+v\n", t.Result)

	var out BlockParsed
	out.Block = t.Result.Block
	out.Uid = uid("Block", out.Hash)
	if len(t.Result.Miner) > 0 {
		out.Miner = Account{
			Uid:     uid("Account", t.Result.Miner),
			Address: t.Result.Miner,
		}
	}
	for _, t := range t.Result.Transactions {
		var outTxn TransactionParsed
		outTxn.Transaction = t.Transaction
		outTxn.From = Account{
			Uid:     uid("Account", t.From),
			Address: t.From,
		}
		outTxn.To = Account{
			Uid:     uid("Account", t.To),
			Address: t.To,
		}
		outTxn.Uid = uid("Transaction", outTxn.Hash)
		out.Transactions = append(out.Transactions, outTxn)
	}

	// out.Transactions = out.Transactions[:1]
	// data, err = json.MarshalIndent(out, " ", " ")
	// Check(err)
	// fmt.Printf("After parsing block:\n%s\n", data)
	return &out, nil
}

func processBlock(gid int, wg *sync.WaitGroup) {
	defer wg.Done()

	var writer io.Writer
	fi, err := os.Stat(*dst)
	if err != nil {
		// This must be GraphQL endpoint.
	} else if fi.IsDir() {
		f, err := os.Create(filepath.Join(*dst, fmt.Sprintf("%02.json.gz", gid)))
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

	for num := range c.blockCh {
		block, err := fetchBlock(num)
		if err != nil {
			fmt.Printf("Unable to parse block: %d\n", num)
			Check(err)
		}
		if writer != nil {
			data, err := json.Marshal(block)
			Check(err)
			_, err = writer.Write(data)
			Check(err)
			return
		}
	}
}

func Check(err error) {
	if err != nil {
		log.Fatalf("Got error: %v", err)
	}
}

func main() {
	flag.Parse()

	fmt.Println("vim-go")
	fmt.Printf("fetch block with error: %v\n", fetchBlock(31706775))
}
