package lambda

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/golang/glog"
	wasmer "github.com/wasmerio/wasmer-go/wasmer"
)

type ExecResult struct {
	RequestId int32
	Res       []byte
}

type WasmInstance struct {
	requestId int32
	// Could be combined to one
	execChan  chan ExecResult
	errorChan chan ExecResult

	mu *sync.Mutex

	guest    *wasmer.Memory
	instance *wasmer.Instance

	mux *http.ServeMux

	start     func(...interface{}) (interface{}, error)
	execute   func(...interface{}) (interface{}, error)
	allocate  func(...interface{}) (interface{}, error)
	setResult func(...interface{}) (interface{}, error)
}

func NewWasmInstance(mux *http.ServeMux) *WasmInstance {

	wasmInstance := &WasmInstance{
		execChan:  make(chan ExecResult),
		errorChan: make(chan ExecResult),
		mu:        &sync.Mutex{},
		mux:       mux,
	}

	return wasmInstance
}

func (w *WasmInstance) LoadScript(script []byte) error {
	// Compiles the module
	module, err := wasmer.NewModule(store, script)
	if err != nil {
		glog.Error(err)
		return err
	}

	importObject, err := wasiEnv.GenerateImportObject(store, module)
	if err != nil {
		glog.Error(err)
		importObject = wasmer.NewImportObject()
	}

	w.registerFunctions(importObject)

	instance, err := wasmer.NewInstance(module, importObject)
	if err != nil {
		glog.Error(err)
		return err
	}
	w.registerExports(instance)

	// Script has been successfuly loaded, close previous instance
	if w.instance != nil {
		w.instance.Close()
	}
	w.instance = instance

	w.Start()

	return nil
}

func (w *WasmInstance) Stop() {
	if w.instance != nil {
		w.instance.Close()
	}
}

func (w *WasmInstance) registerFunctions(importObject *wasmer.ImportObject) {
	log := wasmer.NewFunction(
		store,
		wasmer.NewFunctionType(wasmer.NewValueTypes(wasmer.I32, wasmer.I32), wasmer.NewValueTypes()),
		w.log,
	)
	importObject.Register(
		"env",
		map[string]wasmer.IntoExtern{
			"Log": log,
		},
	)
	execError := wasmer.NewFunction(
		store,
		wasmer.NewFunctionType(wasmer.NewValueTypes(wasmer.I32, wasmer.I32, wasmer.I32), wasmer.NewValueTypes()),
		w.execError,
	)
	importObject.Register(
		"env",
		map[string]wasmer.IntoExtern{
			"execError": execError,
		},
	)
	execResponse := wasmer.NewFunction(
		store,
		wasmer.NewFunctionType(wasmer.NewValueTypes(wasmer.I32, wasmer.I32, wasmer.I32), wasmer.NewValueTypes()),
		w.execResponse,
	)
	importObject.Register(
		"env",
		map[string]wasmer.IntoExtern{
			"execResponse": execResponse,
		},
	)
	do := wasmer.NewFunction(
		store,
		wasmer.NewFunctionType(wasmer.NewValueTypes(wasmer.I32, wasmer.I32), wasmer.NewValueTypes(wasmer.I32)),
		w.do,
	)
	importObject.Register(
		"env",
		map[string]wasmer.IntoExtern{
			"do": do,
		},
	)
}

func (w *WasmInstance) registerExports(instance *wasmer.Instance) {
	guest, err := instance.Exports.GetMemory("memory")
	if err != nil {
		fmt.Println(err)
		return
	}
	w.guest = guest

	execute, err := instance.Exports.GetFunction("execute")
	if err != nil {
		fmt.Println(err)
		return
	}
	w.execute = execute

	allocate, err := instance.Exports.GetFunction("allocate")
	if err != nil {
		fmt.Println(err)
		return
	}
	w.allocate = allocate

	setResult, err := instance.Exports.GetFunction("setResult")
	if err != nil {
		fmt.Println(err)
		return
	}
	w.setResult = setResult

	start, err := instance.Exports.GetWasiStartFunction()
	if err == nil {
		w.start = start
	}
}

func (w *WasmInstance) Start() {
	if w.start != nil {
		w.start()
	}
}

func (w *WasmInstance) Execute(request []byte) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.allocate != nil && w.execute != nil {
		requestId := atomic.AddInt32(&w.requestId, 1)

		ptr, err := w.store(request)
		if err != nil {
			glog.Error(err)
			return nil, err
		}

		go w.execute(ptr, len(request), requestId)

		select {
		case res := <-w.execChan:
			if requestId == res.RequestId {
				return res.Res, nil
			}
		case res := <-w.errorChan:
			if requestId == res.RequestId {
				return nil, errors.New(string(res.Res))
			}
		}
	}
	return nil, errors.New("allocate or execute function not found")
}

func (w *WasmInstance) log(args []wasmer.Value) ([]wasmer.Value, error) {
	ptr := int64(args[0].I32())
	len := int64(args[1].I32())

	data := w.guest.Data()
	fmt.Printf("Log: %s\n", string(data[ptr:ptr+len]))
	return []wasmer.Value{}, nil
}

func (w *WasmInstance) execError(args []wasmer.Value) ([]wasmer.Value, error) {
	requestId := args[0].I32()
	ptr := int64(args[1].I32())
	len := int64(args[2].I32())

	data := w.guest.Data()
	w.errorChan <- ExecResult{
		RequestId: requestId,
		Res:       data[ptr : ptr+len],
	}
	return []wasmer.Value{}, nil
}

func (w *WasmInstance) execResponse(args []wasmer.Value) ([]wasmer.Value, error) {
	requestId := args[0].I32()
	ptr := int64(args[1].I32())
	len := int64(args[2].I32())

	data := w.guest.Data()
	w.execChan <- ExecResult{
		RequestId: requestId,
		Res:       data[ptr : ptr+len],
	}
	return []wasmer.Value{}, nil
}

func (w *WasmInstance) do(args []wasmer.Value) ([]wasmer.Value, error) {
	data := w.guest.Data()

	ptr := int64(args[0].I32())
	length := int64(args[1].I32())

	reqJson := data[ptr : ptr+length]

	req := &HttpRequest{}

	err := json.Unmarshal(reqJson, &req)
	if err != nil {
		glog.Error(err.Error())
	}

	var buf bytes.Buffer
	buf.Write([]byte(req.Body))

	request, err := http.NewRequest(req.Method, req.Url, &buf)
	if err != nil {
		glog.Error(err.Error())
	}
	request.Header = http.Header{}
	for k, a := range req.Header {
		for _, v := range a {
			request.Header.Add(k, v)
		}
	}

	// relative
	var res []byte
	if strings.HasPrefix(req.Url, "/") {
		writer := NewWasmResponseWriter()
		w.mux.ServeHTTP(writer, request)
		res = writer.body
	} else {
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			w.storeResult([]byte(err.Error()))
			return []wasmer.Value{wasmer.NewI32(0)}, nil
		}
		res, err = io.ReadAll(response.Body)
		if err != nil {
			w.storeResult([]byte(err.Error()))
			return []wasmer.Value{wasmer.NewI32(0)}, nil
		}
	}
	w.storeResult(res)
	return []wasmer.Value{wasmer.NewI32(1)}, nil
}

func (w *WasmInstance) storeResult(data []byte) error {
	ptr, err := w.store(data)
	if err != nil {
		return err
	}
	_, err = w.setResult(ptr, len(data))
	return err
}

func (w *WasmInstance) store(data []byte) (int, error) {
	p, err := w.allocate(len(data))
	if err != nil {
		return 0, err
	}
	ptr := int(p.(int32))
	memory := w.guest.Data()
	copy(memory[ptr:ptr+len(data)], data)

	return ptr, nil
}
