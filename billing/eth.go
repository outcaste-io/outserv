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

	"github.com/ethereum/go-ethereum"
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
	endpoint string
	// client   *ethclient.Client
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
	WalletDefaults = `dir=; password=;`
	CloudEndpoint  = "https://cloudflare-eth.com"
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

	// The ETH address is registered in ENS via outcaste.io.
	addr, err := ens.Resolve(client, "outcaste.io")
	if err != nil {
		glog.Errorf("While resolving ENS address for outcaste.io: %v", err)
	} else {
		glog.Infof("Found outcaste.io ETH address: %s", addr.Hex())
	}
	glog.Infof("Wallet is successfully initialized with keystore: %v", EthKeyStorePath)

	return &EthWallet{
		endpoint: ethEndpoint,
		account:  accs[0],
		secret:   password,
		ks:       ks,
	}
}

func initWallet() {
	if len(EthKeyStorePath) == 0 {
		glog.Infof("Cannot initialize wallet because no key store is provided.")
		return
	}
	wallet = NewWallet(EthKeyStorePath, EthKeyStorePassword, CloudEndpoint)
}

func UsdToWei(usd float64) *big.Int {
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
	return weis
}

func weiToEth(a *big.Int) float64 {
	pow12 := new(big.Int).SetUint64(1e12)
	out := new(big.Int).Div(a, pow12)
	return float64(out.Uint64()) / 1e6
}

func (w *EthWallet) Pay(ctx context.Context, usd float64) error {
	if w == nil {
		return errors.New("Wallet is not initialized")
	}

	// It's better to dial just before paying, so we are sure to have a working
	// connection.
	client, err := ethclient.Dial(w.endpoint)
	if err != nil {
		return errors.Wrapf(err, "while dialing ETH endpoint")
	}

	// The ETH address is registered in ENS as outcaste.io.
	addr, err := ens.Resolve(client, "outcaste.io")
	if err != nil {
		glog.Errorf("Error while trying to resolve outcaste.io: %v", err)
		// Fall back on this hardcoded ETH address. Useful for testing.
		addr = common.HexToAddress("0xf707cad56bd75aaa2d2e2e00cf7a221ecd80baf2")
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

	wei := UsdToWei(usd)
	glog.Infof("Charging %.2f USD using %.6f ETH (%d WEI)", usd, weiToEth(wei), wei)

	estGas, err := client.EstimateGas(ctx, ethereum.CallMsg{
		From:     w.account.Address,
		To:       &addr,
		Value:    wei,
		GasPrice: gasPrice,
	})
	if err != nil {
		glog.Warningf("While estimating gas, got error: %v . Setting to 21000 Wei.", err)
		estGas = 21000
	}
	gasLimit := uint64(float64(estGas) * 1.5)
	glog.Infof("Estimated gas: %d. Setting gas limit to %d.", estGas, gasLimit)

	estGasWei := new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(estGas))
	glog.Infof("Estimated cost of txn in ETH: %.6f", weiToEth(estGasWei))

	updatedWei := new(big.Int).Sub(wei, estGasWei)
	glog.Infof("Updating charge from %.6f ETH to %.6f ETH", weiToEth(wei), weiToEth(updatedWei))

	var data []byte
	tx := types.NewTransaction(nonce, addr, updatedWei, gasLimit, gasPrice, data)

	chainID, err := client.NetworkID(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to get chainID")
	}
	signedTx, err := w.ks.SignTxWithPassphrase(w.account, w.secret, tx, chainID)
	if err != nil {
		return errors.Wrap(err, "Failed to sign the transaction.")
	}
	glog.Infof("Attempting ETH txn: %s", signedTx.Hash().Hex())
	return client.SendTransaction(ctx, signedTx)
}
