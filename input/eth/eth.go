package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

var path = flag.String("geth", "", "Geth IPC path")

func main() {
	flag.Parse()

	client, err := ethclient.Dial(*path)
	if err != nil {
		log.Fatal(err)
	}

	account := common.HexToAddress("0xad0e5c205e8c1f6dbeae4cf8b69b693390c13617")
	balance, err := client.BalanceAt(context.Background(), account, nil)
	if err != nil {
		log.Fatalf("balance without block: %v", err)
	}
	fmt.Println("bal", balance) // 25893180161173005034

	// blockNumber := big.NewInt(5532993)
	// balanceAt, err := client.BalanceAt(context.Background(), account, blockNumber)
	// if err != nil {
	// 	log.Fatalf("balance at error: %v", err)
	// }
	// fmt.Println(balanceAt) // 25729324269165216042

	fbalance := new(big.Float)
	fbalance.SetString(balance.String())
	ethValue := new(big.Float).Quo(fbalance, big.NewFloat(math.Pow10(18)))
	fmt.Println("eth bal", ethValue) // 25.729324269165216041

	pendingBalance, err := client.PendingBalanceAt(context.Background(), account)
	fmt.Println("pending", pendingBalance) // 25729324269165216042

	address := common.HexToAddress("0xad0e5c205e8c1f6dbeae4cf8b69b693390c13617")

	fmt.Println(address.Hex())        // 0x71C7656EC7ab88b098defB751B7401B5f6d8976F
	fmt.Println(address.Hash().Hex()) // 0x00000000000000000000000071c7656ec7ab88b098defb751b7401b5f6d8976f
	fmt.Println(address.Bytes())      // [113 199 101 110 199 171 136 176 152 222 251 117 27 116 1 181 246 216 151 111]

	header, err := client.HeaderByNumber(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Header by number", header.Number.String()) // 5671744

	for i := int64(0); i < 100000; i++ {
		blockNumber := big.NewInt(i)
		block, err := client.BlockByNumber(context.Background(), blockNumber)
		if err != nil {
			log.Fatal(err)
		}
		if len(block.Transactions()) == 0 {
			continue
		}

		// fmt.Println(block.Number().Uint64())     // 5671744
		// fmt.Println(block.Difficulty().Uint64()) // 3217000136609065
		// fmt.Println(block.Hash().Hex())          // 0x9e8751ebb5069389b855bba72d94902cc385042661498a415979b7b6ee9ba4b9
		// fmt.Println(len(block.Transactions()))   // 144

		// count, err := client.TransactionCount(context.Background(), block.Hash())
		// if err != nil {
		// 	log.Fatal(err)
		// }
		fmt.Printf("txn count: %d for block: %d\n", len(block.Transactions()), blockNumber)

		// for i, tx := range block.Transactions() {
		// 	if i > 10 {
		// 		break
		// 	}
		// 	fmt.Println(tx.Hash().Hex())        // 0x5d49fcaa394c97ec8a9c3e7bd9e8388d420fb050a52083ca52ff24b3b65bc9c2
		// 	fmt.Println(tx.Value().String())    // 10000000000000000
		// 	fmt.Println(tx.Gas())               // 105000
		// 	fmt.Println(tx.GasPrice().Uint64()) // 102000000000
		// 	fmt.Println(tx.Nonce())             // 110644
		// 	fmt.Println(tx.Data())              // []
		// 	fmt.Println(tx.To().Hex())          // 0x55fE59D8Ad77035154dDd0AD0388D09Dd4047A8e

		// 	// receipt, err := client.TransactionReceipt(context.Background(), tx.Hash())
		// 	// if err != nil {
		// 	// 	fmt.Printf("txn: %x couldn't query receipt. Error: %v\n", tx.Hash(), err)
		// 	// 	continue
		// 	// }

		// 	// fmt.Println("receipt:", receipt.Status) // 1
		// 	// fmt.Println("logs:", receipt.Logs)      // ...
		// }
	}
}
