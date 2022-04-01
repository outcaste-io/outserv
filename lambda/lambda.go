package lambda

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/golang/glog"
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
}

// This is the struct that contains general lambda information
type Lambda struct {
	mu           sync.RWMutex
	instanceId   uint64
	lambdaScript *LambdaScript
	wasmInstance *WasmInstance
}

func init() {
	coordinator = &Coordinator{
		instances: make(map[uint64]*Lambda),
	}
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
			instanceId: instance,
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
	if l.wasmInstance == nil {
		i, err := NewWasmInstance()
		if err != nil {
			return err
		}
		l.wasmInstance = i
	}
	if err := l.wasmInstance.LoadScript(lambdaScript.Script); err != nil {
		return err
	}
	l.lambdaScript = lambdaScript
	return nil
}

func (l *Lambda) SetEmptyScript(lambdaScript *LambdaScript) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.wasmInstance != nil {
		l.wasmInstance.Stop()
	}
	l.lambdaScript = lambdaScript
}

func (l *Lambda) Execute(body interface{}) (interface{}, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.wasmInstance == nil {
		return nil, fmt.Errorf("no lambda script loaded for lambda instance %d", l.instanceId)
	}

	var b []byte
	var err error
	if body != nil {
		b, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}

	glog.Info("Executing lambda")
	res, err := l.wasmInstance.Execute(b)
	if err != nil {
		return nil, err
	}

	var result interface{}
	err = json.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
