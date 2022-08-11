package main

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
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
	Method      string  `json:"method"`

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

const contractAddr = "0x0000000000000000000000000000000000001010"

func (b *Block) fillViaClient() {
	var block *types.Block
	blockNumber := big.NewInt(b.Number)
	var err error
	for i := 0; i < 3; i++ {
		block, err = client.BlockByNumber(context.Background(), blockNumber)
		if err == nil {
			break
		}
		fmt.Printf("Got error while fetching Block %d: %v. Retrying...\n", b.Number, err)
	}
	if err != nil {
		fmt.Printf("Ignoring block: %d\n", b.Number)
		return
	}

	b.Hash = block.Hash().Hex()
	b.Timestamp = int64(block.Time())

	signer := types.LatestSignerForChainID(chainID)

	for _, tx := range block.Transactions() {
		if tx.To() == nil || tx.To().String() != contractAddr {
			continue
		}
		input := tx.Data()
		if len(input) < 4 {
			continue
		}
		method := input[:4]
		m, err := contractAbi.MethodById(method)
		if err != nil {
			fmt.Printf("Unable to parse method with id: %x\n", method)
			// Reject the txn.
			continue
		}

		in := make(map[string]interface{})
		err = m.Inputs.UnpackIntoMap(in, input[4:])
		Check(err)

		receipt, err := client.TransactionReceipt(context.Background(), tx.Hash())
		Check(err)
		gasUsed := new(big.Int).SetUint64(receipt.GasUsed)
		if receipt.Status != 1 {
			// Skip failed transactions.
			continue
		}

		var to, from Account
		if msg, err := tx.AsMessage(signer, nil); err == nil {
			from.Address = msg.From().Hex()
		} else {
			fmt.Printf("Unable to parse from: %v\n", err)
		}

		if tx.To() != nil {
			to.Address = tx.To().Hex()
		}
		if addr, has := in["to"]; has {
			addra := addr.(common.Address)
			to.Address = addra.Hex()
		}

		value := tx.Value()
		if val, has := in["value"]; has {
			value = val.(*big.Int)
		}

		valGwei := new(big.Int).Div(value, gwei)
		fee := new(big.Int).Mul(tx.GasPrice(), gasUsed)
		feeGwei := new(big.Int).Div(fee, gwei)
		txn := Txn{
			Hash:        tx.Hash().Hex(),
			Value:       valGwei.Int64(),
			Fee:         feeGwei.Int64(),
			BlockNumber: b.Number,
			Block:       Block{Hash: b.Hash},
			To:          to,
			From:        from,
			Timestamp:   b.Timestamp,
			Method:      m.Name,
		}
		// data, err := json.MarshalIndent(txn, " ", " ")
		// Check(err)
		// fmt.Printf("Got txn\n%s\n", data)

		if len(txn.To.Address) == 0 || len(txn.From.Address) == 0 {
			continue
		}
		b.Transactions = append(b.Transactions, txn)
	}
}
