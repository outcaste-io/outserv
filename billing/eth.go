// Copyright 2022 Outcaste LLC. Licensed under the Sustainable License v1.0.

package billing

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
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
)

type EthWallet struct {
	endpoint string
	// client   *ethclient.Client
	account accounts.Account
	secret  string
	ks      *keystore.KeyStore
}

var (
	wallet   *EthWallet
	ethToWei = big.NewFloat(1e18)
	gwei     = big.NewInt(1e9)

	EthKeyStorePath     string
	EthKeyStorePassword string
)

const (
	// TODO: Update the endpoints to use outcaste.io domain.
	WalletDefaults = `dir=; password=;`
)

func NewWallet(keyStorePath, password string) *EthWallet {
	ks := keystore.NewKeyStore(keyStorePath, keystore.StandardScryptN, keystore.StandardScryptP)
	accs := ks.Accounts()

	if len(accs) != 1 {
		glog.Fatalf("Found %d wallets in the keystore, expecting one", len(accs))
	}

	end := getEthEndpoint()
	glog.Infof("Wallet initialized with keystore: %v and endpoint: %v", keyStorePath, end)
	return &EthWallet{
		endpoint: end,
		account:  accs[0],
		secret:   password,
		ks:       ks,
	}
}

func getEthEndpoint() string {
	records, err := net.LookupTXT("_eth.outcaste.io")
	// It's OK for us to check fail here, considering we're creating a new
	// wallet, which happens at the beginning of the program.
	x.Checkf(err, "Unable to lookup _eth.outcaste.io")
	x.AssertTruef(len(records) > 0, "Unable to get ETH endpoint")
	return records[0]
}

func initWallet() {
	if len(EthKeyStorePath) == 0 {
		glog.Warningf("Cannot initialize wallet because no key store is provided.")
		return
	}
	wallet = NewWallet(EthKeyStorePath, EthKeyStorePassword)
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
func weiToGwei(a *big.Int) float64 {
	pow6 := big.NewInt(1e6)
	out := new(big.Int).Div(a, pow6)
	return float64(out.Uint64()) / 1e3
}

// Pay method charges the provided USD amount using the Ethereum blockchain.
// User must have Ether in the wallet for this to succeed.
//
// The way it works is:
// 1. Resolve outcaste.io to the Ethereum address.
// 2. Figure out the ETH -> USD conversion rate via Coinbase APIs.
// 3. Calculate the ETH amount we need to charge.
// 4. Estimate the gas fee.
// 5. Deduct the fee from the amount to avoid the user paying it.
// 6. Charge via blockchain.
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

	var addr common.Address
	records, err := net.LookupTXT("_ens.outcaste.io")
	if err != nil || len(records) == 0 || !strings.Contains(records[0], "=") {
		glog.Errorf("Error while trying to resolve _ens.outcaste.io: %v", err)
		// Fall back on this hardcoded ETH address. Useful for testing.
		addr = common.HexToAddress("0xf707cad56bd75aaa2d2e2e00cf7a221ecd80baf2")
	} else {
		hex := strings.Split(records[0], "=")[1]
		addr = common.HexToAddress(hex)
		glog.Infof("Found address: %v\n", addr)
	}
	x.AssertTrue(len(addr) > 0)

	nonce, err := client.PendingNonceAt(ctx, w.account.Address)
	if err != nil {
		return errors.Wrap(err, "Failed to get pending nonce.")
	}
	glog.Infof("Got nonce: %d\n", nonce)

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to get gas price")
	}
	glog.Infof("Got gas price: %.3f Gwei\n", weiToGwei(gasPrice))

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

	var data []byte
	tx := types.NewTransaction(nonce, addr, wei, gasLimit, gasPrice, data)

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
