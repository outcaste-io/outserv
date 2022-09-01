package main

import "encoding/json"

type Block struct {
	Hash         string           `json:"hash,omitempty"`
	Transactions []TransactionOut `json:"transactions,omitempty"`
	Logs         []Log            `json:"logs,omitempty"`
	Uncles       json.RawMessage  `json:"uncles,omitempty"`
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
	LogsBloom            string `json:"logsBloom,omitempty"`
	Logs                 []Log  `json:"logs,omitempty"`

	// Fields picked from Receipt.
	ContractAddress   string `json:"contractAddress,omitempty"`
	CumulativeGasUsed string `json:"cumulativeGasUsed,omitempty"`
	// EffectiveGasPrice string `json:"effectiveGasPrice,omitempty"` // = GasPrice
	GasUsed string `json:"gasUsed,omitempty"` // != Gas
	Status  string `json:"status,omitempty"`
}
type TransactionIn struct {
	Transaction
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}
type TransactionOut struct {
	Transaction
	Fee  string   `json:"fee,omitempty"`
	From *Account `json:"from,omitempty"`
	To   *Account `json:"to,omitempty"`
}

type Account struct {
	Uid     string `json:"uid,omitempty"`
	Address string `json:"address,omitempty"`
}

type Log struct {
	Address          string   `json:"address,omitempty"`
	Topics           []string `json:"topics,omitempty"`
	Data             string   `json:"data,omitempty"`
	BlockNumber      string   `json:"blockNumber,omitempty"`
	TransactionIndex string   `json:"transactionIndex,omitempty"`
	LogIndex         string   `json:"logIndex,omitempty"`
	Removed          bool     `json:"removed"`

	Lid         string       `json:"lid,omitempty"`
	Transaction *Transaction `json:"transaction,omitempty"`
	Block       *Block       `json:"block,omitempty"`
}
