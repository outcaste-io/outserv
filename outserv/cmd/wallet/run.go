// Copyright 2022 Outcaste LLC. Licensed under the Sustainable License v1.0.

package wallet

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/billing"
	"github.com/outcaste-io/outserv/x"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/term"
)

var (
	// Wallet is the sub-command invoked when running "outserv wallet".
	Wallet x.SubCommand
)

func init() {
	Wallet.Cmd = &cobra.Command{
		Use:   "wallet",
		Short: "Ethereum wallet to use with Outserv",
		Run: func(cmd *cobra.Command, args []string) {
			if err := run(Wallet.Conf); err != nil {
				log.Fatalf("%v\n", err)
			}
		},
		Annotations: map[string]string{"group": "tool"},
	}
	Wallet.Cmd.SetHelpTemplate(x.NonRootTemplate)

	flag := Wallet.Cmd.Flags()
	flag.BoolP("create", "c", false, "Create a new wallet.")
	flag.BoolP("test", "t", false, "Test the provided wallet.")
	flag.StringP("password", "p", "", "Passphrase to encrypt the wallet.")
	flag.StringP("dir", "d", "./wallet", "Directory of the wallet keystore.")
}

func run(conf *viper.Viper) error {
	passPhrase := conf.GetString("password")
	dir := conf.GetString("dir")

	if len(passPhrase) == 0 {
		for {
			pass := readPassword("Please enter password for the wallet: ")
			if len(pass) == 0 {
				fmt.Println()
				return fmt.Errorf("Empty password")
			}
			passVerify := readPassword("Please re-enter password for the wallet: ")
			fmt.Println()
			if pass != passVerify {
				fmt.Print("\n\tError: Password mismatch. Please retry...\n")
				continue
			}
			passPhrase = pass
			break
		}
	}
	if conf.GetBool("create") {
		return createWallet(dir, passPhrase)
	}
	if conf.GetBool("test") {
		return testWallet(dir, passPhrase)
	}
	return fmt.Errorf("Need one of --create or --test")
}

func testWallet(keyStoreDir, password string) error {
	wallet := billing.NewWallet(keyStoreDir, password, billing.TestEndpoint)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err := wallet.Pay(ctx, 10.0) // Send $10.
	glog.Infof("ETH payment done with error: %v\n", err)
	return err
}

func createWallet(keyStoreDir, passphrase string) error {
	ks := keystore.NewKeyStore(keyStoreDir, keystore.StandardScryptN, keystore.StandardScryptP)
	accs := ks.Accounts()
	if len(accs) > 0 {
		return errors.New("Keystore already has an account")
	}

	acc, err := ks.NewAccount(passphrase)
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("OUTPUT:")
	fmt.Printf("	Created an Ethereum account with address: %s\n", acc.Address.Hex())
	fmt.Printf("	Wallet JSON file stored in directory: %s\n", keyStoreDir)
	fmt.Println(`
WARNING:
	Please keep the generated JSON file and the passphrase safe and secure.
	If you lose either of those, the funds in this account would be lost forever.
`)
	return nil
}

func readPassword(prompt string) string {
	fmt.Printf("\n%s", prompt)
	p, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		glog.Fatalf("Failed to get passphrase for wallet, %v", err)
	}
	return string(p)
}
