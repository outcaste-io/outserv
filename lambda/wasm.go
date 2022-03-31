package lambda

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	wasmer "github.com/wasmerio/wasmer-go/wasmer"
)

// One page per routine; if we want more we could grow the amount of pages dynamically
const MAX_PAGES uint32 = 4

var store *wasmer.Store
var wasiEnv *wasmer.WasiEnvironment

func init() {
	engine := wasmer.NewEngine()
	store = wasmer.NewStore(engine)

	// For now we use one standard environment for all instances
	wasiEnv, _ = wasmer.NewWasiStateBuilder("lambda").Finalize()
}

type WasmInstance struct {
	avail    []bool
	pageLock *sync.Mutex
	memory   *wasmer.Memory
	guest    *wasmer.Memory
	instance *wasmer.Instance

	buf int

	start   func(...interface{}) (interface{}, error)
	execute func(...interface{}) (interface{}, error)
}

func NewWasmInstance() (*WasmInstance, error) {
	// 4 pages for four concurrent routines
	limits, err := wasmer.NewLimits(1, MAX_PAGES)
	if err != nil {
		return nil, err
	}
	memory := wasmer.NewMemory(store, wasmer.NewMemoryType(limits))
	wasmInstance := &WasmInstance{memory: memory, avail: make([]bool, MAX_PAGES), pageLock: &sync.Mutex{}}

	return wasmInstance, nil
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
}

func (w *WasmInstance) registerExports(instance *wasmer.Instance) {
	guest, err := instance.Exports.GetMemory("memory")
	if err != nil {
		fmt.Println(err)
		return
	}
	w.guest = guest

	getBuffer, err := instance.Exports.GetFunction("GetBuffer")
	if err != nil {
		fmt.Println(err)
		return
	}
	buffer, err := getBuffer()
	if err != nil {
		fmt.Println(err)
		return
	}
	w.buf = int(buffer.(int32))

	execute, err := instance.Exports.GetFunction("execute")
	if err != nil {
		fmt.Println(err)
		return
	}
	w.execute = execute

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
	if w.execute != nil {
		page, err := w.getAvailablePage()
		if err != nil {
			w.releasePage(page)
			return nil, err
		}
		addr, err := w.writeToMemory(page, request)
		if err != nil {
			w.releasePage(page)
			return nil, err
		}

		r, err := w.execute(addr, len(request))
		if err != nil {
			w.releasePage(page)
			return nil, err
		}
		w.releasePage(page)
		data := w.readMemory(page, int(r.(int32)))
		w.releasePage(page)
		return data, nil
	}
	return nil, errors.New("execute function not found")
}

func (w *WasmInstance) getAvailablePage() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		w.pageLock.Lock()
		for page := 0; page < len(w.avail); page++ {
			if !w.avail[page] {
				w.avail[page] = true
				ctx.Done()
				w.pageLock.Unlock()
				return page, nil
			}
		}
		w.pageLock.Unlock()
		if ctx.Err() != nil {
			cancel()
			return 0, errors.New("Could not acquire page for wasm execution")
		}
		glog.Error("Watiing?")
		time.Sleep(10 * time.Millisecond)
	}
}

func (w *WasmInstance) releasePage(page int) {
	w.pageLock.Lock()
	defer w.pageLock.Unlock()
	w.avail[page] = false
}

func (w *WasmInstance) log(args []wasmer.Value) ([]wasmer.Value, error) {
	ptr := int64(args[0].I32())
	len := int64(args[1].I32())

	data := w.guest.Data()
	glog.Error(ptr, len)
	fmt.Printf("Log: %s\n", string(data[ptr:ptr+len]))
	return []wasmer.Value{}, nil
}

func (w *WasmInstance) writeToMemory(page int, data []byte) (int, error) {
	pages := w.guest.Size()
	size := pages.ToUint32()

	if size <= uint32(page) && uint32(page) < MAX_PAGES {
		// Grow to max size for now
		w.guest.Grow(wasmer.Pages(MAX_PAGES - size))
	}
	if len(data) > int(pages.ToBytes()) {
		return 0, errors.New("data does not fit into one page")
	}

	memory := w.guest.Data()
	addr := page*int(pages.ToBytes()) + w.buf

	copy(memory[addr:addr+int(len(data))], data)

	return addr, nil
}

func (w *WasmInstance) readMemory(page int, len int) []byte {
	pages := w.memory.Size()
	memory := w.guest.Data()
	addr := page*int(pages.ToBytes()) + w.buf

	data := make([]byte, len)
	copy(data, memory[addr:addr+len])

	return data
}
