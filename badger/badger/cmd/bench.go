// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package cmd

import (
	"github.com/spf13/cobra"
)

var benchCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Benchmark Badger database.",
	Long: `This command will benchmark Badger for different usecases. 
	Useful for testing and performance analysis.`,
}

func init() {
	RootCmd.AddCommand(benchCmd)
}
