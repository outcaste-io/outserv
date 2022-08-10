package ix

import "time"

type RateMonitor struct {
	start       time.Time
	lastSent    uint64
	lastCapture time.Time
	rates       []float64
	idx         int
}

func NewRateMonitor(numSamples int) *RateMonitor {
	return &RateMonitor{
		start: time.Now(),
		rates: make([]float64, numSamples),
	}
}

const minRate = 0.0001

// Capture captures the current number of sent bytes. This number should be monotonically
// increasing.
func (rm *RateMonitor) Capture(sent uint64) {
	diff := sent - rm.lastSent
	dur := time.Since(rm.lastCapture)
	rm.lastCapture, rm.lastSent = time.Now(), sent

	rate := float64(diff) / dur.Seconds()
	if rate < minRate {
		rate = minRate
	}
	rm.rates[rm.idx] = rate
	rm.idx = (rm.idx + 1) % len(rm.rates)
}

// Rate returns the average rate of transmission smoothed out by the number of samples.
func (rm *RateMonitor) Rate() uint64 {
	var total float64
	var den float64
	for _, r := range rm.rates {
		if r < minRate {
			// Ignore this. We always set minRate, so this is a zero.
			// Typically at the start of the rate monitor, we'd have zeros.
			continue
		}
		total += r
		den += 1.0
	}
	if den < minRate {
		return 0
	}
	return uint64(total / den)
}
