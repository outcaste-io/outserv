// Copyright 2022 Outcaste LLC. Licensed under the Smart License v1.0.

package billing

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/wealdtech/go-ens/v3"
)

type ethWallet struct {
	account accounts.Account
	secret  string
	ks      *keystore.KeyStore
}

var (
	wallet              *ethWallet
	errMultipleAccounts = errors.New("Multiple eth account found")

	EthKeyStorePath     string
	EthKeyStorePassword string
)

const WalletDefaults = `keystore=; password=;`

func initWallet() {
	if len(EthKeyStorePath) == 0 {
		glog.Infof("Cannot initialize wallet because no key store is provided.")
		return
	}

	ks := keystore.NewKeyStore(EthKeyStorePath, keystore.StandardScryptN, keystore.StandardScryptP)
	accs := ks.Accounts()

	if len(accs) != 1 {
		glog.Fatalf("Found %d wallets in the keystore, expecting one", len(accs))
	}
	wallet = &ethWallet{
		account: accs[0],
		secret:  EthKeyStorePassword,
		ks:      ks,
	}
	glog.Infof("Wallet is successfully initialized with keystore: %v", EthKeyStorePath)
}

func (w *ethWallet) Pay(ctx context.Context, wei *big.Int) error {
	if w == nil {
		return errors.New("Wallet is not initialized")
	}

	client, err := ethclient.Dial(ethEndpoint)
	if err != nil {
		return errors.Wrap(err, "Failed to establish connection.")
	}

	// The ETH address is registered in ENS via outcaste.io.
	addr, err := ens.Resolve(client, "outcaste.io")
	if err != nil {
		return errors.Wrapf(err, "Error while resolving outcaste.io ENS address")
	}
	glog.Infof("Found outcaste.io ETH address: %s", addr.Hex())

	nonce, err := client.PendingNonceAt(ctx, w.account.Address)
	if err != nil {
		return errors.Wrap(err, "Failed to get pending nonce.")
	}

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to get gas price")
	}

	var data []byte
	// Set gas limit to 21K.
	tx := types.NewTransaction(nonce, addr, wei, gasLimit, gasPrice, data)

	chainID, err := client.NetworkID(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to get chainID")
	}
	signedTx, err := w.ks.SignTxWithPassphrase(w.account, w.secret, tx, chainID)
	if err != nil {
		return errors.Wrap(err, "Failed to sign the transaction.")
	}
	return client.SendTransaction(ctx, signedTx)
}
