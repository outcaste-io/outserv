package lambda

import (
	"errors"
	"fmt"

	"github.com/golang/glog"
)

var coordinator *Coordinator

type LambdaScript struct {
	ID     string `json:"id,omitempty"`
	Script []byte `json:"script,omitempty"`
	Hash   string `json:"hash,omitempty"`
}

type Coordinator struct {
	instances map[uint64]*Lambda
}

// This is the struct that contains general lambda information
type Lambda struct {
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
	// Lock
	lambda, ok := coordinator.instances[instance]
	if !ok {
		lambda = &Lambda{
			instanceId: instance,
		}
		coordinator.instances[instance] = lambda
	}
	// Unlock
	return lambda
}

func (l *Lambda) LoadScript(lambdaScript *LambdaScript) error {
	// Lock
	if l.lambdaScript != nil && l.lambdaScript.Hash == lambdaScript.Hash {
		return errors.New("lambda script already loaded")
	}
	if l.wasmInstance == nil {
		i, err := NewWasmInstance(lambdaScript.Script)
		if err != nil {
			return err
		}
		l.wasmInstance = i
	} else {
		if err := l.wasmInstance.UpdateScript(lambdaScript.Script); err != nil {
			return err
		}
	}
	l.lambdaScript = lambdaScript
	return nil
	// Unlock
}

func (l *Lambda) SetEmptyScript(lambdaScript *LambdaScript) {
	l.wasmInstance.Stop()
	l.lambdaScript = lambdaScript
}

func (l *Lambda) Execute() (string, error) {
	// Lock
	if l.wasmInstance == nil {
		return "", errors.New(fmt.Sprintf("no lambda script loaded for lambda instance %d", l.instanceId))
	}
	glog.Info("Executing lambda")
	return l.wasmInstance.Execute("")
	// Unlock
}
