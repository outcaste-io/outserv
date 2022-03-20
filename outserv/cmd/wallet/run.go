package wallet

import (
	"errors"
	"log"
	"os"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/x"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	logger = log.New(os.Stderr, "", 0)
	// Wallet is the sub-command invoked when running "outserv wallet".
	Wallet x.SubCommand
)

func init() {
	Wallet.Cmd = &cobra.Command{
		Use:   "wallet",
		Short: "Create an ethereum account",
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
			pass := readPassword("Please enter passphrase of the wallet.")
			passVerify := readPassword("Please re-enter passphrase of the wallet.")
			if pass != passVerify {
				glog.Info("Passphrase didn't match. Please retry...")
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

	glog.Infof("Created an account with address: %s\n", acc.Address.Hex())
	return nil
}

func readPassword(prompt string) string {
	glog.Info(prompt)
	p, err := terminal.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		glog.Fatalf("Failed to get passphrase for wallet, %v", err)
	}
	return string(p)
}
