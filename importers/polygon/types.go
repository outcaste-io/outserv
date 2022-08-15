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

type BlockOut struct {
	Block
	Uid          string           `json:"uid,omitempty"`
	Miner        *AccountOut      `json:"miner,omitempty"`
	Transactions []TransactionOut `json:"transactions,omitempty"`
}

type AccountOut struct {
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

	// Fields picked from Receipt.
	ContractAddress   string `json:"contractAddress,omitempty"`
	CumulativeGasUsed string `json:"cumulativeGasUsed,omitempty"`
	// EffectiveGasPrice string `json:"effectiveGasPrice,omitempty"` // = GasPrice
	GasUsed string `json:"gasUsed,omitempty"` // != Gas
	Status  string `json:"status,omitempty"`
}
type TransactionRPC struct {
	Transaction
	TransactionHash string   `json:"transactionHash,omitempty"`
	BlockHash       string   `json:"blockHash,omitempty"`
	From            string   `json:"from,omitempty"`
	To              string   `json:"to,omitempty"`
	Logs            []LogRPC `json:"logs,omitempty"`
}
type TransactionOut struct {
	Transaction
	Uid  string      `json:"uid,omitempty"`
	Fee  string      `json:"fee,omitempty"`
	From *AccountOut `json:"from,omitempty"`
	To   *AccountOut `json:"to,omitempty"`
	Logs []LogOut    `json:"logs,omitempty"`
}

type Log struct {
	Address          string   `json:"address,omitempty"`
	Topics           []string `json:"topics,omitempty"`
	Data             string   `json:"data,omitempty"`
	BlockNumber      string   `json:"blockNumber,omitempty"`
	TransactionIndex string   `json:"transactionIndex,omitempty"`
	LogIndex         string   `json:"logIndex,omitempty"`
	Removed          bool     `json:"removed,omitempty"`
}
type LogRPC struct {
	Log
	TransactionHash string `json:"transactionHash,omitempty"`
	BlockHash       string `json:"blockHash,omitempty"`
}
type LogOut struct {
	Log
	Uid         string         `json:"uid,omitempty"`
	Transaction TransactionOut `json:"transaction,omitempty"`
	Block       BlockOut       `json:"block,omitempty"`
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
