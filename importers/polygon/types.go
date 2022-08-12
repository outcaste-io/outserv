package main

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
	Address string
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
