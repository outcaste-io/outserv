// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/dustin/go-humanize"
	"github.com/outcaste-io/outserv/badger/badger/cmd"
	"github.com/outcaste-io/ristretto/z"
	"go.opencensus.io/zpages"
)

func main() {
	go func() {
		for i := 8080; i < 9080; i++ {
			fmt.Printf("Listening for /debug HTTP requests at port: %d\n", i)
			if err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", i), nil); err != nil {
				fmt.Println("Port busy. Trying another one...")
				continue

			}
		}
	}()
	zpages.Handle(nil, "/z")
	runtime.SetBlockProfileRate(100)
	runtime.GOMAXPROCS(128)

	out := z.CallocNoRef(1, "Badger.Main")
	fmt.Printf("jemalloc enabled: %v\n", len(out) > 0)
	z.StatsPrint()
	z.Free(out)

	cmd.Execute()
	fmt.Printf("Num Allocated Bytes at program end: %s\n",
		humanize.IBytes(uint64(z.NumAllocBytes())))
	if z.NumAllocBytes() > 0 {
		fmt.Println(z.Leaks())
	}
}
