package main

import (
	"bytes"
	"log"
	"unicode/utf8"
)

type Block struct {
	Hash             string  `json:"hash,omitempty"`
	Number           string  `json:"number,omitempty"`
	BaseFeePerGas    string  `json:"baseFeePerGas,omitempty"`
	Difficulty       string  `json:"difficulty,omitempty"`
	ExtraData        string  `json:"extraData,omitempty"`
	GasLimit         string  `json:"gasLimit,omitempty"`
	GasUsed          string  `json:"gasUsed,omitempty"`
	LogsBloom        string  `json:"logsBloom,omitempty"`
	MixHash          string  `json:"mixHash,omitempty"`
	Nonce            string  `json:"nonce,omitempty"`
	ParentHash       string  `json:"parentHash,omitempty"`
	ReceiptsRoot     string  `json:"receiptsRoot,omitempty"`
	Sha3Uncles       string  `json:"sha3Uncles,omitempty"`
	Size             string  `json:"size,omitempty"`
	StateRoot        string  `json:"stateRoot,omitempty"`
	Timestamp        string  `json:"timestamp,omitempty"`
	TotalDifficulty  string  `json:"totalDifficulty,omitempty"`
	TransactionsRoot string  `json:"transactionsRoot,omitempty"`
	Uncles           []Block `json:"uncles,omitempty"`
}

type BlockRPC struct {
	Block
	Miner        string
	Transactions []TransactionRPC
}

type BlockParsed struct {
	Uid string `json:"uid,omitempty"`
	Block
	Miner        Account             `json:"miner,omitempty"`
	Transactions []TransactionParsed `json:"transactions,omitempty"`
}

type Account struct {
	Uid     string `json:"uid,omitempty"`
	Address string `json:"address,omitempty"`
}

type Transaction struct {
	Hash                 string `json:"hash,omitempty"`
	BlockNumber          string `json:"blockNumber,omitempty"`
	Gas                  string `json:"gas,omitempty"`
	GasPrice             string `json:"gasPrice,omitempty"`
	MaxFeePerGas         string `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas,omitempty"`
	Input                string `json:"input,omitempty"`
	Nonce                string `json:"nonce,omitempty"`
	TransactionIndex     string `json:"transactionIndex,omitempty"`
	Value                string `json:"value,omitempty"`
	Type                 string `json:"type,omitempty"`
	ChainId              string `json:"chainId,omitempty"`
	V                    string `json:"v,omitempty"`
	R                    string `json:"r,omitempty"`
	S                    string `json:"s,omitempty"`
}

type TransactionRPC struct {
	Transaction
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

type TransactionParsed struct {
	Uid string `json:"uid,omitempty"`
	Transaction
	From Account `json:"from,omitempty"`
	To   Account `json:"to,omitempty"`
}

const quote = '"'

// toGraphQLInputX does NOT handle a {"key": 1.0} yet. But, we don't expect
// those from JSON-RPC anyway.
func toGraphQLInputX(data []byte) []byte {
	var depth, key int
	out := &bytes.Buffer{}
	out.Grow(len(data))

	var idx int
	next := func() rune {
		cur, sz := utf8.DecodeRune(data[idx:])
		idx += sz // Move it past the parsed char.
		return cur
	}

	for idx < len(data) {
		cur := next()

		switch {
		case cur == '{' || cur == '[':
			out.WriteRune(cur)
			depth++
			key = 0
		case cur == '}' || cur == ']':
			out.WriteRune(cur)
			depth--
			if depth == 0 {
				return out.Bytes()
			}
		case depth > 0 && key == 0 && cur == quote:
			// Found a key
			start := idx
			for {
				if ch := next(); ch == quote {
					break
				}
			}
			// Got end quote
			out.Write(data[start : idx-1])
			key = 1
		case key == 1 && cur == quote:
			// Found a value
			start := idx
			for {
				ch := next()
				if ch == '\\' {
					next()
				} else if ch == quote {
					break
				}
			}
			out.Write(data[start-1 : idx]) // include the quotes.
			key = 0
		default:
			out.WriteRune(cur)
		}
	}
	log.Fatalf("Invalid end of input: %s\n", data)
	return nil
}
