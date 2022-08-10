package main

import (
	"context"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/outcaste-io/outserv/importers/ix"
)

type Account struct {
	Uid     string `json:"uid,omitempty"`
	Address string `json:"address,omitempty"`
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
	Uid          string   `json:"uid,omitempty"`
	Hash         string   `json:"hash,omitempty"`
	Number       int64    `json:"number,omitempty"`
	Timestamp    int64    `json:"timestamp,omitempty"`
	Transactions []Txn    `json:"transactions,omitempty"`
	OmmerCount   int      `json:"ommerCount,omitempty"`
	Ommers       []Block  `json:"ommers,omitempty"`
	Miner        *Account `json:"miner,omitempty"`

	// The following fields are used by ETH. But, not part of Outserv's GraphQL Schema.
	TimestampStr string `json:"ts,omitempty"`
}

func (b *Block) Wait() { b.wg.Wait() }
func (b *Block) Fill() {
	b.fillViaClient()
	b.wg.Done()
}

func (b *Block) fillViaClient() {
	blockNumber := big.NewInt(b.Number)
	block, err := client.BlockByNumber(context.Background(), blockNumber)
	ix.Check(err)
	for _, tx := range block.Transactions() {
		receipt, err := client.TransactionReceipt(context.Background(), tx.Hash())
		ix.Check(err)
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
