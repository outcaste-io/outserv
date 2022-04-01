package wasm

import (
	"context"
)

// 5 pages of data space
var buf [5 * 65536]byte

//export GetBuffer
func GetBuffer() *byte {
	return &buf[0]
}

//export execute
func execute(req string) int {
	ctx := context.Background()

	request, err := UnmarshalRequest([]byte(req))
	if err != nil {
		// We need a way to send back errors
		// We could write to an error buffer and indicate an error with result -1
		Log(err.Error())
		return 0
	}
	if router == nil {
		Log("no router defined")
		return 0
	}
	res, err := router.resolve(ctx, request)
	if err != nil {
		Log(err.Error())
		return 0
	}

	return Respond(string(res))
}
