// Copyright 2022 Outcaste LLC. Licensed under the Smart License v1.0.

package billing

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
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

// - On start we should create an account if it doesn't exist.
// - Only the leader should create the account and propose it to the followers, otherwise each node
// will end up having its own account.

// Initialize keystore.
// Create an account if it doesn't exist.
func InitWallet(keyStoreDir, passphrase string) error {
	ks := keystore.NewKeyStore(keyStoreDir, keystore.StandardScryptN, keystore.StandardScryptP)
	accs := ks.Accounts()
	if len(accs) > 1 {
		// Should not reach here.
		return errMultipleAccounts
	}

	wallet = &ethWallet{
		ks:     ks,
		secret: passphrase,
	}

	// Found an existing account.
	if len(accs) == 1 {
		wallet.account = accs[0]
		return nil
	}

	// No account found, create one and set to wallet.
	acc, err := ks.NewAccount(passphrase)
	if err != nil {
		return err
	}
	wallet.account = acc
	return nil
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
