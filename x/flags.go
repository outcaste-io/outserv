// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package x

import (
	"github.com/outcaste-io/ristretto/z"
	"github.com/spf13/pflag"
)

const (
	TraceDefaults     = `ratio=0.01; jaeger=; datadog=;`
	TelemetryDefaults = `reports=true;`
)

// FillCommonFlags stores flags common to Alpha and Zero.
func FillCommonFlags(flag *pflag.FlagSet) {
	flag.String("my", "",
		"addr:port of this server, so other Dgraph servers can talk to this.")

	// OpenCensus flags.
	//
	// datadog: See https://github.com/DataDog/opencensus-go-exporter-datadog/issues/34
	//          about the status of supporting annotation logs through the datadog exporter
	flag.String("trace", TraceDefaults, z.NewSuperFlagHelp(TraceDefaults).
		Head("Trace options").
		Flag("ratio",
			"The ratio of queries to trace.").
		Flag("jaeger",
			"URL of Jaeger to send OpenCensus traces.").
		Flag("datadog",
			"URL of Datadog to send OpenCensus traces. As of now, the trace exporter does not "+
				"support annotation logs and discards them.").
		String())

	flag.String("survive", "process",
		`Choose between "process" or "filesystem".`+"\n    "+
			`If set to "process", there would be no data loss in case of process crash, but `+
			`the behavior would be indeterministic in case of filesystem crash.`+"\n    "+
			`If set to "filesystem", blocking sync would be called after every write, hence `+
			`guaranteeing no data loss in case of hard reboot.`+"\n    "+
			`Most users should be OK with choosing "process".`)

	flag.String("telemetry", TelemetryDefaults, z.NewSuperFlagHelp(TelemetryDefaults).
		Head("Telemetry (diagnostic) options").
		Flag("reports",
			"Send anonymous telemetry data to Dgraph devs.").
		String())
}
