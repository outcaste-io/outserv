// Copyright 2022 Outcaste LLC. Licensed under the Smart License v1.0.

package billing

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	"github.com/shirou/gopsutil/v3/process"
)

func Run(closer *z.Closer) {
	go trackCPU(closer)
}

// One CPU is always considered to be used.
func minOne(a float64) float64 {
	if a < 1.0 {
		return 1.0
	}
	return a
}

var microCpuHour int64

const USDPerCpuHour = 0.03 // 3 cents per cpu-hour.
const Minute = time.Minute // Using this indirection for debugging.

func CPUHours() float64 {
	mch := atomic.LoadInt64(&microCpuHour)
	return float64(mch) / 1e6
}

func AccountedFor(cpuHours float64) {
	mch := int64(cpuHours * 1e6)
	atomic.AddInt64(&microCpuHour, -mch)
}

func trackCPU(closer *z.Closer) {
	defer closer.Done()

	glog.Infof("Got PID for usage tracking: %d\n", os.Getpid())
	proc, err := process.NewProcess(int32(os.Getpid()))
	x.Checkf(err, "unable to track process")

	// We need to call it upfront, so all the following calls would track CPU
	// usage correctly. That's because every Percent call tracks the CPU usage
	// since the last Percent call.
	_, err = proc.Percent(0)
	x.Checkf(err, "unable to track CPU usage")

	tick := time.NewTicker(Minute)
	defer tick.Stop()

	for i := int64(1); ; i++ {
		select {
		case <-tick.C: // Track CPU usage every minute.
			usage, err := proc.Percent(0)
			x.Checkf(err, "unable to track CPU usage")

			usage = usage / 100.0 // Convert percentage to the number of cpus.
			usage = minOne(usage) // Minimum usage of one cpu.
			usage = usage / 60    // 60 mins in the hour.
			atomic.AddInt64(&microCpuHour, int64(usage*1e6))

		case <-closer.HasBeenClosed():
			glog.Infof("Billing exiting usage tracking.")
			return
		}
	}
}

// Charge returns the amount charged for in dollars, and error if any.
func Charge(cpuHours float64) error {
	// TODO: Fill out this function to charge
	usd := cpuHours * USDPerCpuHour
	glog.Infof("Charged $%.3f for %.3f CPU hours\n", usd, cpuHours)
	return nil
}
