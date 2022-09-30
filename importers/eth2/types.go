package main

import "encoding/json"

type Block struct {
	Hash             string            `json:"hash,omitempty"`
	Number           string            `json:"number,omitempty"`
	BaseFeePerGas    string            `json:"baseFeePerGas,omitempty"`
	Difficulty       string            `json:"difficulty,omitempty"`
	ExtraData        string            `json:"extraData,omitempty"`
	GasLimit         string            `json:"gasLimit,omitempty"`
	GasUsed          string            `json:"gasUsed,omitempty"`
	LogsBloom        string            `json:"logsBloom,omitempty"`
	MixHash          string            `json:"mixHash,omitempty"`
	Nonce            string            `json:"nonce,omitempty"`
	ParentHash       string            `json:"parentHash,omitempty"`
	ReceiptsRoot     string            `json:"receiptsRoot,omitempty"`
	Sha3Uncles       string            `json:"sha3Uncles,omitempty"`
	Size             string            `json:"size,omitempty"`
	StateRoot        string            `json:"stateRoot,omitempty"`
	Timestamp        string            `json:"timestamp,omitempty"`
	TotalDifficulty  string            `json:"totalDifficulty,omitempty"`
	TransactionsRoot string            `json:"transactionsRoot,omitempty"`
	Transactions     []*TransactionOut `json:"transactions,omitempty"`
	// Logs             []Log             `json:"logs,omitempty"`
}

type BlockOut struct {
	Block
	Type       string     `json:"@type,omitempty"`
	Miner      *Account   `json:"miner,omitempty"`
	Ommers     []BlockOut `json:"ommers,omitempty"`
	OmmerCount string     `json:"ommerCount,omitempty"`
}

func (b *BlockOut) MarshalJSON() ([]byte, error) {
	b.Type = "Block"
	type Alias BlockOut
	return json.Marshal(Alias(*b))
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
	Root              string `json:"root,omitempty"` // PostState
	ContractAddress   string `json:"contractAddress,omitempty"`
	CumulativeGasUsed string `json:"cumulativeGasUsed,omitempty"`
	// EffectiveGasPrice string `json:"effectiveGasPrice,omitempty"` // = GasPrice
	GasUsed string `json:"gasUsed,omitempty"` // != Gas
	Status  string `json:"status,omitempty"`
}

type TransactionOut struct {
	Transaction
	Type string   `json:"@type,omitempty"`
	Fee  string   `json:"fee,omitempty"`
	From *Account `json:"from,omitempty"`
	To   *Account `json:"to,omitempty"`
}

func (t *TransactionOut) MarshalJSON() ([]byte, error) {
	t.Type = "Transaction"
	type Alias TransactionOut
	return json.Marshal(Alias(*t))
}

type Account struct {
	Type    string `json:"@type"`
	Address string `json:"address,omitempty"`
}

func (a *Account) MarshalJSON() ([]byte, error) {
	// type Alias BlockOut
	a.Type = "Account"
	type Alias Account
	return json.Marshal(Alias(*a))
}

type Log struct {
	Address          string   `json:"address,omitempty"`
	Topics           []string `json:"topics,omitempty"`
	Data             string   `json:"data,omitempty"`
	BlockNumber      string   `json:"blockNumber,omitempty"`
	TransactionIndex string   `json:"transactionIndex,omitempty"`
	LogIndex         string   `json:"logIndex,omitempty"`
	Removed          bool     `json:"removed,omitempty"`

	Type string `json:"@type,omitempty"`
	// Lid         string       `json:"lid,omitempty"`
	Transaction *Transaction `json:"transaction,omitempty"`
	Block       *Block       `json:"block,omitempty"`
}

func (l *Log) MarshalJSON() ([]byte, error) {
	l.Type = "Log"
	type Alias Log
	return json.Marshal(Alias(*l))
}

type Marker struct {
	ProcessedBlock      int64  `json:"ProcessedBlock,omitempty"`
	ProcessedBlockHash  string `json:"ProcessedBlockHash,omitempty"`
	LatestGethBlock     int64  `json:"LatestGethBlock,omitempty"`
	LatestGethBlockHash string `json:"LatestGethBlockHash,omitempty"`
	Timestamp           string `json:"ts,omitempty"`
}

type GethBockResponse struct {
	Data struct {
		Block struct {
			Number int64  `json:"number"`
			Hash   string `json:"hash"`
		} `json:"block"`
	} `json:"data"`
}
