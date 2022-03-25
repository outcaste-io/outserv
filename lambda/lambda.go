package lambda

import (
	"encoding/base64"

	"github.com/golang/glog"
)

// For now this is a packaged variable instead of a class object
// Simplifies things for now

type LambdaScript struct {
	ID     string `json:"id,omitempty"`
	Script []byte `json:"script,omitempty"`
	Hash   string `json:"hash,omitempty"`
}

var lambda *Lambda

// This is the struct that contains general lambda information
type Lambda struct {
	wasmInstance *WasmInstance
}

func (l *Lambda) Execute() {
	l.wasmInstance.execute()
}

func (l *Lambda) LoadScript(lambdaScript *LambdaScript) error {
	glog.Info("Loading Lambda Script")

	return nil
	wasmScript, err := base64.StdEncoding.DecodeString(string(lambdaScript.Script))
	if err != nil {
		return err
	}

	if l.wasmInstance == nil {
		l.wasmInstance, err = NewWasmInstance(wasmScript)
		if err != nil {
			return err
		}
	} else {
		return l.wasmInstance.UpdateScript(wasmScript)
	}

	return nil
}

func GetLambda() *Lambda {
	if lambda == nil {
		lambda = &Lambda{}
	}
	return lambda
}
