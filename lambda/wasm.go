package lambda

import (
	"errors"
	"fmt"

	wasmer "github.com/wasmerio/wasmer-go/wasmer"
)

type WasmInstance struct {
	store    *wasmer.Store
	memory   *wasmer.Memory
	instance *wasmer.Instance

	bufferPtr   int
	responsePtr int

	start   func(...interface{}) (interface{}, error)
	execute func(...interface{}) (interface{}, error)
}

func NewWasmInstance(code []byte) (*WasmInstance, error) {
	engine := wasmer.NewEngine()
	store := wasmer.NewStore(engine)

	// Compiles the module
	module, err := wasmer.NewModule(store, code)
	if err != nil {
		return nil, err
	}

	wasiEnv, err := wasmer.NewWasiStateBuilder("wasi-program").
		// Choose according to your actual situation
		// Argument("--foo").
		// Environment("ABC", "DEF").
		// MapDirectory("./", ".").
		Finalize()
	if err != nil {
		return nil, err
	}

	// Instantiates the module
	importObject, err := wasiEnv.GenerateImportObject(store, module)
	if err != nil {
		importObject = wasmer.NewImportObject()
	}

	wasmInstance := &WasmInstance{store: store}

	wasmInstance.init(module, importObject)

	return wasmInstance, nil
}

// Very unclean
// Handle all errors
// When update was not successful stick to the old instance
func (w *WasmInstance) UpdateScript(script []byte) error {
	module, _ := wasmer.NewModule(w.store, script)
	wasiEnv, err := wasmer.NewWasiStateBuilder("wasi-program").
		// Choose according to your actual situation
		// Argument("--foo").
		// Environment("ABC", "DEF").
		// MapDirectory("./", ".").
		Finalize()
	if err != nil {
		return err
	}

	// Instantiates the module
	importObject, err := wasiEnv.GenerateImportObject(w.store, module)
	if err != nil {
		importObject = wasmer.NewImportObject()
	}

	w.instance.Close()
	w.init(module, importObject)

	return err
}

func (w *WasmInstance) Stop() {

}

func (w *WasmInstance) init(module *wasmer.Module, importObject *wasmer.ImportObject) error {
	w.registerFunctions(importObject)

	instance, err := wasmer.NewInstance(module, importObject)
	if err != nil {
		return err
	}
	w.instance = instance

	w.registerExports()

	return nil
}

func (w *WasmInstance) registerFunctions(importObject *wasmer.ImportObject) {
	log := wasmer.NewFunction(
		w.store,
		wasmer.NewFunctionType(wasmer.NewValueTypes(wasmer.I32, wasmer.I32), wasmer.NewValueTypes()),
		w.log,
	)
	importObject.Register(
		"env",
		map[string]wasmer.IntoExtern{
			"Log": log,
		},
	)
}

func (w *WasmInstance) registerExports() {
	mem, err := w.instance.Exports.GetMemory("memory")
	if err != nil {
		panic(fmt.Sprintln("failed get memory:", err))
	}
	w.memory = mem

	getBuffer, err := w.instance.Exports.GetFunction("getBuffer")
	if err != nil {
		fmt.Println(err)
	}

	buffer, err := getBuffer()
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Println(buffer)
	w.bufferPtr = int(buffer.(int32))

	getResult, err := w.instance.Exports.GetFunction("getResult")
	if err != nil {
		fmt.Println(err)
	}

	resBuffer, err := getResult()
	if err != nil {
		fmt.Println(err)
	}
	w.responsePtr = int(resBuffer.(int32))

	execute, err := w.instance.Exports.GetFunction("execute")
	if err != nil {
		fmt.Println(err)
		return
	}
	w.execute = execute

	start, err := w.instance.Exports.GetWasiStartFunction()
	if err != nil {
		fmt.Println(err)
	}
	w.start = start
}

func (w *WasmInstance) Start() {
	if w.start != nil {
		w.start()
	}
}

func (w *WasmInstance) Execute(request string) (string, error) {
	if w.execute != nil {
		data := w.memory.Data()
		copy(data[w.bufferPtr:w.bufferPtr+len(request)], ([]byte)(request))

		r, err := w.execute(w.bufferPtr, len(request), w.responsePtr)
		if err != nil {
			return "", err
		}
		res := string(data[w.responsePtr : w.responsePtr+int(r.(int32))])
		return res, nil
	}
	return "", errors.New("Execute function not found")
}

func (w *WasmInstance) log(args []wasmer.Value) ([]wasmer.Value, error) {
	ptr := int64(args[0].I32())
	len := int64(args[1].I32())
	data := w.memory.Data()

	fmt.Printf("Log: %s\n", string(data[ptr:ptr+len]))
	return []wasmer.Value{}, nil
}
