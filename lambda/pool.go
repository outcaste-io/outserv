package lambda

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/wasmerio/wasmer-go/wasmer"
)

var store *wasmer.Store
var wasiEnv *wasmer.WasiEnvironment

func init() {
	engine := wasmer.NewEngine()
	store = wasmer.NewStore(engine)

	// For now we use one standard environment for all instances
	wasiEnv, _ = wasmer.NewWasiStateBuilder("lambda").Finalize()
}

// Maybe this will replace the single WasmInstance
type InstancePool struct {
	idle     []*WasmInstance
	active   []*WasmInstance
	capacity int
	loadLock *sync.Mutex
	mulock   *sync.Mutex
	wg       *sync.WaitGroup

	script []byte
	mux    *http.ServeMux
}

//InitPool Initialize the pool
func NewInstancePool(capacity uint32, mux *http.ServeMux) *InstancePool {
	poolObjects := make([]*WasmInstance, 0)
	active := make([]*WasmInstance, 0)
	pool := &InstancePool{
		idle:     poolObjects,
		active:   active,
		capacity: int(capacity),
		mulock:   new(sync.Mutex),
		loadLock: new(sync.Mutex),
		wg:       &sync.WaitGroup{},
		mux:      mux,
	}
	return pool
}

func (p *InstancePool) SetScript(script []byte) {
	p.loadLock.Lock()
	defer p.loadLock.Unlock()

	p.script = script
	p.wg.Wait()
	for _, instance := range p.idle {
		instance.Stop()
	}

	p.idle = make([]*WasmInstance, 0)
}

func (p *InstancePool) Get() (*WasmInstance, error) {
	// Can't get instance while loading script
	p.loadLock.Lock()
	defer p.loadLock.Unlock()

	p.mulock.Lock()
	defer p.mulock.Unlock()

	if len(p.idle) == 0 && len(p.active) >= p.capacity {
		return nil, fmt.Errorf("no pool object free. Please request after sometime")
	}
	if len(p.idle) == 0 {
		instance := NewWasmInstance(p.mux)
		if err := instance.LoadScript(p.script); err != nil {
			return nil, err
		}
		p.idle = append(p.idle, instance)
	}

	obj := p.idle[0]
	p.idle = p.idle[1:]
	p.active = append(p.active, obj)
	p.wg.Add(1)

	return obj, nil
}

func (p *InstancePool) Return(instance *WasmInstance) error {
	p.mulock.Lock()
	defer p.mulock.Unlock()
	err := p.remove(instance)
	if err != nil {
		return err
	}
	p.idle = append(p.idle, instance)
	p.wg.Done()
	return nil
}

func (p *InstancePool) remove(instance *WasmInstance) error {
	currentActiveLength := len(p.active)
	for i, obj := range p.active {
		if obj == instance {
			p.active[currentActiveLength-1], p.active[i] = p.active[i], p.active[currentActiveLength-1]
			p.active = p.active[:currentActiveLength-1]
			return nil
		}
	}
	return fmt.Errorf("instance doesn't belong to the pool")
}
