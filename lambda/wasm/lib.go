package wasm

import (
	"unsafe"

	"github.com/valyala/fastjson"
)

// 4 pages of data space
var buf [4 * 65536]byte

//export getBuffer
func GetBuffer() *byte {
	return &buf[0]
}

// stores the response in the buffer. res should be a valid json.
func Respond(res string) int {
	ptr := unsafe.Pointer(GetBuffer())

	for i := 0; i < len(res); i++ {
		t := (*uint8)(unsafe.Pointer(uintptr(ptr) + uintptr(i)*unsafe.Sizeof(res[0])))
		*t = res[i]
	}
	return len(res)
}

//export Log
func Log(s string)

// Unmarshals the request into a Request object, until encoding/json is supported
func UnmarshalRequest(data []byte) (*Request, error) {
	t := &Request{}
	var p fastjson.Parser
	v, err := p.Parse(string(data))
	if err != nil {
		return nil, err
	}
	t.AccessToken = string(v.GetStringBytes("X-Dgraph-AccessToken"))
	t.Args = v.GetArray("args")
	t.AuthHeader = AuthHeader{
		Key:   string(v.Get("authHeader").GetStringBytes("key")),
		Value: string(v.Get("authHeader").GetStringBytes("value")),
	}

	// TODO: Webhooks
	//t.Event = &Event{}

	// TODO: Support InfoField
	/*t.Info = InfoField{}
	t.Info.Field.Alias = string(v.Get("info").Get("field").GetStringBytes("alias"))
	t.Info.Field.Name = string(v.Get("info").Get("field").GetStringBytes("name"))
	t.Info.Field.Arguments = v.Get("info").Get("field").GetStringBytes("arguments")
	t.Info.Field.Directives = []Directive{}
	t.Info.Field.SelectionSet = []SelectionField{}*/

	t.Parents = v.GetArray("parents")

	t.Resolver = string(v.GetStringBytes("resolver"))

	return t, nil
}

type AuthHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Directive struct {
	Name      string            `json:"name"`
	Arguments map[string][]byte `json:"arguments"`
}

type SelectionField struct {
	Alias        string           `json:"alias"`
	Name         string           `json:"name"`
	Arguments    []byte           `json:"arguments"`
	Directives   []Directive      `json:"directives"`
	SelectionSet []SelectionField `json:"slectionSet"`
}

type InfoField struct {
	Field SelectionField `json:"field"`
}

type Request struct {
	AccessToken string            `json:"X-Dgraph-AccessToken"`
	Args        []*fastjson.Value `json:"args"`
	Info        InfoField         `json:"info"`
	AuthHeader  AuthHeader        `json:"authHeader"`
	Resolver    string            `json:"resolver"`
	Parents     []*fastjson.Value `json:"parents"`
	Event       *Event            `json:"event"`
}

type Event struct {
	TypeName  string           `json:"__typename"`
	CommitTs  uint64           `json:"commitTs"`
	Operation string           `json:"operation"`
	Add       *AddEventInfo    `json:"add"`
	Update    *UpdateEventInfo `json:"update"`
	Delete    *DeleteEventInfo `json:"delete"`
}

type AddEventInfo struct {
	RootUIDs []string                 `json:"rootUIDs"`
	Input    []map[string]interface{} `json:"input"`
}

type UpdateEventInfo struct {
	RootUIDs    []string               `json:"rootUIDs"`
	SetPatch    map[string]interface{} `json:"setPatch"`
	RemovePatch map[string]interface{} `json:"removePatch"`
}

type DeleteEventInfo struct {
	RootUIDs []string `json:"rootUIDs"`
}
