// Portions Copyright 2019-2021 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package debuginfo

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/x"
	"github.com/spf13/cobra"
)

type profileOpts struct {
	outservAddr string
	zeroAddr    string
	archive     bool
	directory   string
	seconds     uint32
	metricTypes []string
}

var (
	Profile    x.SubCommand
	profileCmd = profileOpts{}
)

var metricMap = map[string]string{
	"block":        "/debug/pprof/block",
	"cpu":          "/debug/pprof/profile",
	"goroutine":    "/debug/pprof/goroutine?debug=2",
	"health":       "/health",
	"heap":         "/debug/pprof/heap",
	"jemalloc":     "/debug/jemalloc",
	"metrics":      "/metrics",
	"mutex":        "/debug/pprof/mutex",
	"state":        "/state",
	"threadcreate": "/debug/pprof/threadcreate",
	"trace":        "/debug/pprof/trace",
	"vars":         "/debug/vars",
}

var metricList = []string{
	"block",
	"cpu",
	"goroutine",
	"health",
	"heap",
	"jemalloc",
	"metrics",
	"mutex",
	"state",
	"threadcreate",
	"trace",
	"vars",
}

func init() {
	Profile.Cmd = &cobra.Command{
		Use:   "profile",
		Short: "Collect profile and debug info from given Outserv instance",
		Run: func(cmd *cobra.Command, args []string) {
			if err := collectDebugInfo(); err != nil {
				glog.Errorf("error while collecting Outserv profile: %s", err)
				os.Exit(1)
			}
		},
		Annotations: map[string]string{"group": "debug"},
	}

	Profile.EnvPrefix = "OUTSERV_AGENT_DEBUGINFO"
	Profile.Cmd.SetHelpTemplate(x.NonRootTemplate)

	flags := Profile.Cmd.Flags()
	flags.StringVarP(&profileCmd.outservAddr, "outserv", "o", "localhost:8080",
		"HTTP address of running Outserv instance.")
	flags.StringVarP(&profileCmd.directory, "directory", "d", "",
		"Directory to write the debug info into.")
	flags.BoolVarP(&profileCmd.archive, "archive", "x", true,
		"Whether to archive the generated report")
	flags.Uint32VarP(&profileCmd.seconds, "seconds", "s", 30,
		"Duration for time-based metric collection.")
	flags.StringSliceVarP(&profileCmd.metricTypes, "metrics", "m", metricList,
		"List of metrics & profile to dump in the report.")
}

func collectDebugInfo() (err error) {
	if profileCmd.directory == "" {
		profileCmd.directory, err = ioutil.TempDir("/tmp", "outserv-debuginfo")
		if err != nil {
			return fmt.Errorf("error while creating temporary directory: %s", err)
		}
	} else {
		err = os.MkdirAll(profileCmd.directory, 0644)
		if err != nil {
			return err
		}
	}
	glog.Infof("using directory %s for debug info dump.", profileCmd.directory)

	collectDebug()

	if profileCmd.archive {
		return archiveDebugInfo()
	}
	return nil
}

func collectDebug() {
	if profileCmd.outservAddr != "" {
		filePrefix := filepath.Join(profileCmd.directory, "alpha_")

		saveMetrics(profileCmd.outservAddr, filePrefix, profileCmd.seconds, profileCmd.metricTypes)

	}
}

func archiveDebugInfo() error {
	archivePath, err := createGzipArchive(profileCmd.directory)
	if err != nil {
		return fmt.Errorf("error while archiving debuginfo directory: %s", err)
	}

	glog.Infof("Debuginfo archive successful: %s", archivePath)

	if err = os.RemoveAll(profileCmd.directory); err != nil {
		glog.Warningf("error while removing debuginfo directory: %s", err)
	}
	return nil
}
