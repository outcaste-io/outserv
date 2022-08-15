package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/outcaste-io/outserv/lex"
	"github.com/pkg/errors"
)

var url = flag.String("url", "", "Bor URL address")
var dst = flag.String("dst", "", "Outserv GraphQL endpoint, or a directory")
var start = flag.Int64("start", 1, "Starting Block Number")
var end = flag.Int64("end", 0, "If set, stop at this Block Number, including it")
var gor = flag.Int("j", 4, "Num Goroutines to use")
var dryRun = flag.Bool("dry", false, "If true, don't send txns to GraphQL endpoint")

func uid(writer io.Writer, typ, id string) string {
	if writer == nil {
		return ""
	}
	return fmt.Sprintf("_:%s.%s", typ, id)
}

func sendRequest(data []byte) error {
	if *dryRun {
		return nil
	}
	// TODO: Check that the schema is correctly set.
	var wr WResp
	resp, err := http.Post(fmt.Sprintf("%s/graphql", *dst),
		"application/graphql", bytes.NewBuffer(data))
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

func fillReceipt(dst *TransactionOut, writer io.Writer) error {
	q := fmt.Sprintf(`{
	"jsonrpc": "2.0",
	"method": "eth_getTransactionReceipt",
	"params": [%q],
	"id": 1}`, dst.Hash)

	resp, err := http.Post(*url, "application/json", strings.NewReader(q))
	if err != nil {
		return errors.Wrapf(err, "Unable to fetch receipt: %s", dst.Hash)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "Unable to read response")
	}
	fmt.Printf("Got receipt data: %s\n", data)

	type T struct {
		Result TransactionRPC `json:"result"`
	}
	var t T
	Check(json.Unmarshal(data, &t))

	src := t.Result
	Assert(len(src.TransactionHash) > 0)
	if src.TransactionHash != dst.Hash {
		log.Fatalf("Receipt hash: %s . Wanted hash: %s\n", src.TransactionHash, dst.Hash)
		return nil
	}
	fmt.Printf("---------Got result: %+v\n", src)

	Assert(len(dst.Logs) == 0) // Should only fill once.
	for _, l := range src.Logs {
		var lo LogOut
		lo.Log = l.Log
		lo.Uid = uid(writer, "Log", fmt.Sprintf("%s-%s", dst.Hash, l.LogIndex))
		lo.Transaction = TransactionOut{Uid: dst.Uid}
		lo.Block = BlockOut{Uid: uid(writer, "Block", src.BlockHash)}
		dst.Logs = append(dst.Logs, lo)
	}
	dst.ContractAddress = src.ContractAddress
	dst.CumulativeGasUsed = src.CumulativeGasUsed
	// dst.EffectiveGasPrice = src.EffectiveGasPrice
	dst.GasUsed = src.GasUsed
	dst.Status = src.Status

	// Calculate Fee
	if price, ok := new(big.Int).SetString(dst.GasPrice, 0); ok {
		if gasUsed, ok2 := new(big.Int).SetString(dst.GasUsed, 0); ok2 {
			fee := new(big.Int).Mul(price, gasUsed)
			dst.Fee = "0x" + fee.Text(16)
		}
	}
	return nil
}

func fetchBlock(writer io.Writer, blockNumber int64) (*BlockOut, error) {
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

	var out BlockOut
	out.Block = t.Result.Block
	out.Uid = uid(writer, "Block", out.Hash)
	if len(t.Result.Miner) > 0 {
		out.Miner = &AccountOut{
			Uid:     uid(writer, "Account", t.Result.Miner),
			Address: t.Result.Miner,
		}
	}
	for _, t := range t.Result.Transactions {
		var outTxn TransactionOut
		outTxn.Transaction = t.Transaction
		outTxn.From = &AccountOut{
			Uid:     uid(writer, "Account", t.From),
			Address: t.From,
		}
		outTxn.To = &AccountOut{
			Uid:     uid(writer, "Account", t.To),
			Address: t.To,
		}
		outTxn.Uid = uid(writer, "Transaction", outTxn.Hash)
		Check(fillReceipt(&outTxn, writer))
		out.Transactions = append(out.Transactions, outTxn)
	}
	return &out, nil
}

var blockMu = `
mutation {
	addBlock(upsert: true, input: %s) {
		numUids
	}
}
`

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
type Batch struct {
	Blks []BlockOut `json:"Blks,omitempty"`
}
type GQL struct {
	Query     string `json:"query"`
	Variables Batch  `json:"variables"`
}

func isQuote(r rune) bool {
	return r == '"'
}
func isColon(r rune) bool {
	return r == ':'
}
func lexTopLevel(l *lex.Lexer) lex.StateFn {
	r := l.Next()
	if r == '{' {
		l.Depth++
		l.ArgDepth = 0
	} else if r == '}' {
		l.Depth--
		if l.Depth == 0 {
			return nil
		}
	} else if l.Depth > 0 && l.ArgDepth == 0 && r == '"' {
		// This is the key. Do not emit.
		l.Ignore()
		l.Next()
		l.AcceptUntil(isQuote)
		l.Emit(lex.ItemType(2))
		l.Next()
		l.Ignore()
		l.AcceptUntil(isColon)
		l.Next()
		l.Emit(lex.ItemType(2))
		l.ArgDepth++
		return lexTopLevel
	} else if l.ArgDepth > 0 && r == '"' {
		Check(l.LexQuotedString())
		l.Emit(lex.ItemType(2))
		l.ArgDepth--
		return lexTopLevel
	}
	l.Emit(lex.ItemType(2))
	return lexTopLevel
}

func toGraphQLInput(data []byte) {
	fmt.Printf("data of len: %d\n", len(data))
	lex := lex.Lexer{Input: string(data)}
	lex.Run(lexTopLevel)
	itr := lex.NewIterator()
	for itr.Next() {
		item := itr.Item()
		fmt.Printf("Got val: %s\n", item.Val)
	}
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
			data, err := json.Marshal([]BlockOut{*block})
			Check(err)
			// fmt.Printf("Before:\n%s\n", data)
			gqdata := toGraphQLInputX(data)

			q := fmt.Sprintf(blockMu, gqdata)
			// q := GQL{
			// 	Query:     blockMu,
			// 	Variables: Batch{Blks: []BlockParsed{*block}},
			// }
			fmt.Printf("Query:\n%s\n", q)
			Check(sendRequest([]byte(q)))
		}
	}
}

func Check(err error) {
	if err != nil {
		debug.PrintStack()
		log.Fatalf("Got error: %v", err)
	}
}
func Assert(thing bool) {
	if !thing {
		debug.PrintStack()
		log.Fatal("Assertion failed")
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
var graphqlRe *regexp.Regexp

func main() {
	flag.Parse()

	var err error
	graphqlRe, err = regexp.Compile(`\"([a-zA-Z0-9_]+)\":`)
	Check(err)

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
