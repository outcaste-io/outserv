package wallet

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/x"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/term"
)

var (
	logger = log.New(os.Stderr, "", 0)
	// Wallet is the sub-command invoked when running "outserv wallet".
	Wallet x.SubCommand
)

func init() {
	Wallet.Cmd = &cobra.Command{
		Use:   "create-wallet",
		Short: "Create Ethereum wallet with an anonymous account",
		Run: func(cmd *cobra.Command, args []string) {
			if err := run(Wallet.Conf); err != nil {
				logger.Fatalf("%v\n", err)
			}
		},
		Annotations: map[string]string{"group": "tool"},
	}
	Wallet.Cmd.SetHelpTemplate(x.NonRootTemplate)

	flag := Wallet.Cmd.Flags()
	flag.StringP("password", "s", "", "Passphrase to encrypt the wallet.")
	flag.StringP("dir", "d", "./keystore", "Directory of the wallet keystore.")
}

func run(conf *viper.Viper) error {
	passPhrase := conf.GetString("password")
	dir := conf.GetString("dir")

	if len(passPhrase) == 0 {
		for {
			pass := readPassword("Please enter passphrase for the wallet: ")
			passVerify := readPassword("Please re-enter passphrase for the wallet: ")
			if pass != passVerify {
				fmt.Print("\nPassphrase didn't match. Please retry...\n")
				continue
			}
			passPhrase = pass
			break
		}
	}
	return createWallet(dir, passPhrase)
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
