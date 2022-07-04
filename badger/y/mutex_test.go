package y

import (
	"fmt"
	"sync"
	"testing"
)

func BenchmarkRWMutex(b *testing.B) {
	for ng := 1; ng <= 256; ng <<= 2 {
		b.Run(fmt.Sprint(ng), func(b *testing.B) {
			var mu sync.RWMutex
			mu.Lock()

			var wg sync.WaitGroup
			wg.Add(ng)

			n := b.N
			quota := n / ng

			for g := ng; g > 0; g-- {
				if g == 1 {
					quota = n
				}

				go func(quota int) {
					for i := 0; i < quota; i++ {
						mu.RLock()
						mu.RUnlock()
					}
					wg.Done()
				}(quota)

				n -= quota
			}

			if n != 0 {
				b.Fatalf("Incorrect quota assignments: %v remaining", n)
			}

			b.StartTimer()
			mu.Unlock()
			wg.Wait()
			b.StopTimer()
		})
	}
}

func BenchmarkMutex(b *testing.B) {
	for ng := 1; ng <= 256; ng <<= 2 {
		b.Run(fmt.Sprint(ng), func(b *testing.B) {
			var mu sync.Mutex

			var wg sync.WaitGroup
			wg.Add(ng)

			n := b.N
			quota := n / ng

			for g := ng; g > 0; g-- {
				if g == 1 {
					quota = n
				}

				go func(quota int) {
					for i := 0; i < quota; i++ {
						mu.Lock()
						mu.Unlock()
					}
					wg.Done()
				}(quota)

				n -= quota
			}

			if n != 0 {
				b.Fatalf("Incorrect quota assignments: %v remaining", n)
			}

			b.StartTimer()
			wg.Wait()
			b.StopTimer()
		})
	}
}
