// Portions Copyright 2021 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package version

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/outcaste-io/outserv/x"
)

// Version is the sub-command invoked when running "dgraph version".
var Version x.SubCommand

func init() {
	Version.Cmd = &cobra.Command{
		Use:   "version",
		Short: "Prints the Outserv version details",
		Long:  "Version prints the Outserv version as reported by the build details.",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Print(x.BuildDetails())
			os.Exit(0)
		},
		Annotations: map[string]string{"group": "default"},
	}
	Version.Cmd.SetHelpTemplate(x.NonRootTemplate)
}
