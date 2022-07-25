// Copyright 2022 Outcaste LLC. Licensed under the Apache License v2.0.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
)

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

type Block struct {
	wg           sync.WaitGroup
	Uid          string `json:"uid,omitempty"`
	Number       int64  `json:"number"`
	Timestamp    string `json:"timestamp,omitempty"`
	Transactions []Txn  `json:"transactions,omitempty"`
}

// fastFillViaGraphQL connects with the GraphQL endpoint exposed via Geth. And
// fetches blocks and transaction information. This is the fastest way to get
// data out of Geth -- at least 10x faster than the JSON-RPC interface.
//
// To adapt this importer to your needs, please start with modifying here, and
// the Txn struct.
func (b *Block) fastFillViaGraphQL() {
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

// slowFillViaClient doesn't yet have UIDs set. Needs to be modified to match
// with fastFillViaGraphQL. Recommendation is to use fastFillViaGraphQL instead
// because it is indeed 10x faster than this function.
//
// If you're not using fastFillViaGraphQL and you want to adapt this importer to
// your needs, start with modifying this function and the Txn struct.
func (b *Block) slowFillViaClient() {
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
