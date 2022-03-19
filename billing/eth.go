// Copyright 2022 Outcaste LLC. Licensed under the Smart License v1.0.

package billing

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
)

type payInfo struct {
	wei         *big.Int // Amount to be paid
	gasLimit    uint64
	destAddress common.Address

	chainEndpoint string
}

type ethWallet struct {
	account accounts.Account
	secret  string
	ks      *keystore.KeyStore
}

var (
	wallet              *ethWallet
	errMultipleAccounts = errors.New("Multiple eth account found")
)

func init() {
	dir := x.WorkerConfig.EthKeyStorePath
	if len(dir) == 0 {
		return
	}

	ks := keystore.NewKeyStore(dir, keystore.StandardScryptN, keystore.StandardScryptP)
	accs := ks.Accounts()
	if len(accs) != 1 {
		glog.Fatalf("Found %d wallets in the keystore, expecting one", len(accs))
	}
	wallet = &ethWallet{
		account: accs[0],
		secret:  x.WorkerConfig.EthKeyStorePassword,
		ks:      ks,
	}
}

func (w *ethWallet) Pay(ctx context.Context, in *payInfo) error {
	if w == nil {
		return errors.New("Wallet is not initialized")
	}

	client, err := ethclient.Dial(in.chainEndpoint)
	if err != nil {
		return err
	}
	nonce, err := client.PendingNonceAt(ctx, w.account.Address)
	if err != nil {
		return err
	}

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return err
	}

	var data []byte
	tx := types.NewTransaction(nonce, in.destAddress, in.wei, in.gasLimit, gasPrice, data)

	chainID, err := client.NetworkID(ctx)
	if err != nil {
		return err
	}
	signedTx, err := w.ks.SignTxWithPassphrase(w.account, w.secret, tx, chainID)
	if err != nil {
		return err
	}
	return client.SendTransaction(ctx, signedTx)
}
