package wasm

import (
	"context"
)

// return buffer
var result string

//export setResult
func setResult(res string) {
	result = res
}

//export execute
func execute(req string, requestId int32) {
	ctx := context.Background()

	request, err := UnmarshalRequest([]byte(req))
	if err != nil {
		execError(requestId, err.Error())
	}
	if router == nil {
		execError(requestId, "no router defined")
	}
	res, err := router.resolve(ctx, request)
	if err != nil {
		execError(requestId, err.Error())
	}

	execResponse(requestId, string(res))
}

//export execError
func execError(requestId int32, err string)

//export execResponse
func execResponse(requestId int32, res string)

//export allocate
func allocate(length uint64) *byte {
	buf := make([]byte, length)
	return &buf[0]
}
