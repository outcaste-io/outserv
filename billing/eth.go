// Copyright 2022 Outcaste LLC. Licensed under the Smart License v1.0.

package billing

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
	"github.com/wealdtech/go-ens/v3"
)

type EthWallet struct {
	client  *ethclient.Client
	account accounts.Account
	secret  string
	ks      *keystore.KeyStore
}

var (
	wallet              *EthWallet
	errMultipleAccounts = errors.New("Multiple eth account found")
	ethToWei            = big.NewFloat(1e18)

	EthKeyStorePath     string
	EthKeyStorePassword string
)

const (
	WalletDefaults = `keystore=; password=;`
	gasLimit       = uint64(42000) // Based on some online articles.
	MainEndpoint   = "https://mainnet.infura.io/v3/b03d4386493d4ce79ac8eddb29fb2d15"
	TestEndpoint   = "https://rinkeby.infura.io/v3/b03d4386493d4ce79ac8eddb29fb2d15"
)

func NewWallet(keyStorePath, password, ethEndpoint string) *EthWallet {
	ks := keystore.NewKeyStore(keyStorePath, keystore.StandardScryptN, keystore.StandardScryptP)
	accs := ks.Accounts()

	if len(accs) != 1 {
		glog.Fatalf("Found %d wallets in the keystore, expecting one", len(accs))
	}

	client, err := ethclient.Dial(ethEndpoint)
	if err != nil {
		glog.Fatalf("While dialing ETH endpoint: %v", err)
	}

	return &EthWallet{
		client:  client,
		account: accs[0],
		secret:  password,
		ks:      ks,
	}
}

func initWallet() {
	if len(EthKeyStorePath) == 0 {
		glog.Infof("Cannot initialize wallet because no key store is provided.")
		return
	}

	wallet = NewWallet(EthKeyStorePath, EthKeyStorePassword, MainEndpoint)

	// The ETH address is registered in ENS via outcaste.io.
	addr, err := ens.Resolve(wallet.client, "outcaste.io")
	if err != nil {
		glog.Errorf("While resolving ENS address for outcaste.io: %v", err)
	} else {
		glog.Infof("Found outcaste.io ETH address: %s", addr.Hex())
	}
	glog.Infof("Wallet is successfully initialized with keystore: %v", EthKeyStorePath)
}

func UsdToWei(usd float64) (*big.Int, float64) {
	type S struct {
		Base     string `json:"base"`
		Currency string `json:"currency"`
		Amount   string `json:"amount"`
	}
	var ethToUsd float64
	query := func() error {
		resp, err := http.Get("https://api.coinbase.com/v2/prices/ETH-USD/spot")
		if err != nil {
			return errors.Wrapf(err, "while querying Coinbase")
		}
		// Response is something like this:
		// {"data":{"base":"ETH","currency":"USD","amount":"3231.74"}}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrapf(err, "while reading data: %s", resp.Body)
		}
		_ = resp.Body.Close()

		var m map[string]S
		if err := json.Unmarshal(data, &m); err != nil {
			return errors.Wrapf(err, "while unmarshal data: %v", err)
		}
		s, ok := m["data"]
		if !ok {
			return fmt.Errorf("Unable to find 'data' key: %s", data)
		}
		if s.Base != "ETH" || s.Currency != "USD" {
			return fmt.Errorf("Unexpected data from Coinbase: %s", data)
		}
		ethToUsd, err = strconv.ParseFloat(s.Amount, 64)
		if err != nil {
			return errors.Wrapf(err, "unable to parse %q", s.Amount)
		}
		glog.Infof("Got conversion from Coinbase: %+v\n", s)
		return nil
	}

	err := x.RetryUntilSuccess(120, time.Minute, func() error {
		err := query()
		if err != nil {
			glog.Errorf("While checking ETH to USD: %v", err)
		}
		return err
	})
	x.Check(err)
	x.AssertTrue(ethToUsd > 1.0)

	valInEth := usd / ethToUsd

	weiFloat := new(big.Float).Mul(big.NewFloat(valInEth), ethToWei)
	weis, _ := weiFloat.Int(nil)
	return weis, valInEth
}

func (w *EthWallet) Pay(ctx context.Context, usd float64) error {
	if w == nil {
		return errors.New("Wallet is not initialized")
	}

	// The ETH address is registered in ENS as outcaste.io.
	addr, err := ens.Resolve(w.client, "outcaste.io")
	if err != nil {
		glog.Errorf("Error while trying to resolve outcaste.io: %v", err)
		// Fall back on this hardcoded ETH address. Useful for testing.
		addr = common.HexToAddress("0xf707cad56bd75aaa2d2e2e00cf7a221ecd80baf2")
	}
	glog.Infof("Found outcaste.io ETH address: %s", addr.Hex())

	nonce, err := w.client.PendingNonceAt(ctx, w.account.Address)
	if err != nil {
		return errors.Wrap(err, "Failed to get pending nonce.")
	}

	gasPrice, err := w.client.SuggestGasPrice(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to get gas price")
	}

	wei, eth := UsdToWei(usd)
	glog.Infof("Charging %.2f USD using %d WEI ~ %.6f ETH", usd, wei, eth)

	var data []byte
	tx := types.NewTransaction(nonce, addr, wei, gasLimit, gasPrice, data)

	chainID, err := w.client.NetworkID(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to get chainID")
	}
	signedTx, err := w.ks.SignTxWithPassphrase(w.account, w.secret, tx, chainID)
	if err != nil {
		return errors.Wrap(err, "Failed to sign the transaction.")
	}
	glog.Infof("Attempting ETH txn: %s", signedTx.Hash().Hex())
	return w.client.SendTransaction(ctx, signedTx)
}
