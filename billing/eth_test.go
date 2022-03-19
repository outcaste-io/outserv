package billing

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestInitWallet(t *testing.T) {
	ksDir := t.TempDir()
	secret := "secret"

	require.NoError(t, InitWallet(ksDir, secret))

	// Initializing again should not fail
	require.NoError(t, InitWallet(ksDir, secret))
}

func TestPay(t *testing.T) {
	ksDir := t.TempDir()
	secret := "secret"

	require.NoError(t, InitWallet(ksDir, secret))

	toAddress := common.HexToAddress("0x4592d8f8d7b001e72cb26a73e4fa1806a51ac79d")
	pinfo := &payInfo{
		wei:           1000000,
		gasLimit:      1000,
		destAddress:   toAddress,
		chainEndpoint: "/tmp/geth.ipc",
	}

	require.NoError(t, wallet.Pay(context.Background(), pinfo))
}
