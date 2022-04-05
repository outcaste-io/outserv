package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/x"
)

var coordinator *Coordinator

type LambdaScript struct {
	ID     string `json:"id,omitempty"`
	Script []byte `json:"script,omitempty"`
	Hash   string `json:"hash,omitempty"`
}

type Coordinator struct {
	m         sync.Mutex
	instances map[uint64]*Lambda
	mux       *http.ServeMux
}

// This is the struct that contains general lambda information
type Lambda struct {
	mu           sync.RWMutex
	instanceId   uint64
	lambdaScript *LambdaScript
	instancePool *InstancePool
	mux          *http.ServeMux
}

func NewLambdaCoordinator(mux *http.ServeMux) *Coordinator {
	glog.Error(mux)
	coordinator = &Coordinator{
		instances: make(map[uint64]*Lambda),
		mux:       mux,
	}
	return coordinator
}

func (c *Coordinator) Close() error {
	for _, l := range c.instances {
		l.Close()
	}
	return nil
}

func (l *Lambda) GetCurrentScript() (*LambdaScript, bool) {
	return l.lambdaScript, l.lambdaScript != nil
}

func Instance(instance uint64) *Lambda {
	coordinator.m.Lock()
	defer coordinator.m.Unlock()
	lambda, ok := coordinator.instances[instance]
	if !ok {
		lambda = &Lambda{
			instanceId:   instance,
			instancePool: NewInstancePool(x.Config.Lambda.Num, coordinator.mux),
			mux:          coordinator.mux,
		}
		coordinator.instances[instance] = lambda
	}
	return lambda
}

func (l *Lambda) LoadScript(lambdaScript *LambdaScript) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lambdaScript != nil && l.lambdaScript.Hash == lambdaScript.Hash {
		return errors.New("lambda script already loaded")
	}
	l.instancePool.SetScript(lambdaScript.Script)
	l.lambdaScript = lambdaScript
	return nil
}

func (l *Lambda) SetEmptyScript(lambdaScript *LambdaScript) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.instancePool.SetScript(nil)

	l.lambdaScript = lambdaScript
}

func (l *Lambda) Execute(body interface{}) (interface{}, error) {
	var b []byte
	var err error
	if body != nil {
		b, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}

	instance, err := l.getAvailableInstance()
	if err != nil {
		return nil, err
	}

	//glog.Info("Executing lambda")
	res, err := instance.Execute(b)
	if err != nil {
		return nil, err
	}

	if err := l.instancePool.Return(instance); err != nil {
		glog.Warning(err)
	}

	var result interface{}
	err = json.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (l *Lambda) getAvailableInstance() (*WasmInstance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		instance, err := l.instancePool.Get()
		if err == nil {
			return instance, nil
		}
		if ctx.Err() != nil {
			cancel()
			return nil, errors.New("Could not acquire instance for wasm execution")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (l *Lambda) Close() {
	// TODO(schartey/wasm) close
}
