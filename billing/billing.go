// Copyright 2022 Outcaste LLC. Licensed under the Smart License v1.0.

package billing

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/process"
)

func Run(closer *z.Closer) {
	initWallet()
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

const (
	USDPerCpuHour = 0.03        // 3 cents per cpu-hour.
	Minute        = time.Minute // Using this indirection for debugging.

	gasLimit    = uint64(21000) // Based on some online articles.
	ethEndpoint = "https://rinkeby.infura.io"
)

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

var ethToWei = big.NewFloat(1e18)

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

// Charge returns the amount charged for in dollars, and error if any.
func Charge(cpuHours float64) error {
	usd := cpuHours * USDPerCpuHour

	wei := UsdToWei(usd)
	glog.Infof("Charging %.2f USD using %d ETH WEI", usd, wei)
	if err := wallet.Pay(context.Background(), wei); err != nil {
		return err
	}
	glog.Infof("Charged $%.3f for %.3f CPU hours\n", usd, cpuHours)
	return nil
}
