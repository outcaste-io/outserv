// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package alpha

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/outcaste-io/outserv/graphql/admin"
	"github.com/outcaste-io/outserv/protos/pb"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/query"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
)

func allowed(method string) bool {
	return method == http.MethodPost || method == http.MethodPut
}

// Common functionality for these request handlers. Returns true if the request is completely
// handled here and nothing further needs to be done.
func commonHandler(w http.ResponseWriter, r *http.Request) bool {
	// Do these requests really need CORS headers? Doesn't seem like it, but they are probably
	// harmless aside from the extra size they add to each response.
	x.AddCorsHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	if r.Method == "OPTIONS" {
		return true
	} else if !allowed(r.Method) {
		w.WriteHeader(http.StatusBadRequest)
		x.SetStatus(w, x.ErrorInvalidMethod, "Invalid method")
		return true
	}

	return false
}

// Read request body, transparently decompressing if necessary. Return nil on error.
func readRequest(w http.ResponseWriter, r *http.Request) []byte {
	var in io.Reader = r.Body

	if enc := r.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		if enc == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				x.SetStatus(w, x.Error, "Unable to create decompressor")
				return nil
			}
			defer gz.Close()
			in = gz
		} else {
			x.SetStatus(w, x.ErrorInvalidRequest, "Unsupported content encoding")
			return nil
		}
	}

	body, err := ioutil.ReadAll(in)
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return nil
	}

	return body
}

// parseUint64 reads the value for given URL parameter from request and
// parses it into uint64, empty string is converted into zero value
func parseUint64(r *http.Request, name string) (uint64, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return 0, nil
	}

	uintVal, err := strconv.ParseUint(value, 0, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "while parsing %s as uint64", name)
	}

	return uintVal, nil
}

// parseBool reads the value for given URL parameter from request and
// parses it into bool, empty string is converted into zero value
func parseBool(r *http.Request, name string) (bool, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return false, nil
	}

	boolval, err := strconv.ParseBool(value)
	if err != nil {
		return false, errors.Wrapf(err, "while parsing %s as bool", name)
	}

	return boolval, nil
}

// parseDuration reads the value for given URL parameter from request and
// parses it into time.Duration, empty string is converted into zero value
func parseDuration(r *http.Request, name string) (time.Duration, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return 0, nil
	}

	durationValue, err := time.ParseDuration(value)
	if err != nil {
		return 0, errors.Wrapf(err, "while parsing %s as time.Duration", name)
	}

	return durationValue, nil
}

// This method should just build the request and proxy it to the Query method of dgraph.Server.
// It can then encode the response as appropriate before sending it back to the user.
func queryHandler(w http.ResponseWriter, r *http.Request) {
	if commonHandler(w, r) {
		return
	}

	isDebugMode, err := parseBool(r, "debug")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	queryTimeout, err := parseDuration(r, "timeout")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	startTs, err := parseUint64(r, "startTs")
	hash := r.URL.Query().Get("hash")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}

	body := readRequest(w, r)
	if body == nil {
		return
	}

	var params struct {
		Query     string            `json:"query"`
		Variables map[string]string `json:"variables"`
	}

	contentType := r.Header.Get("Content-Type")
	mediaType, contentTypeParams, err := mime.ParseMediaType(contentType)
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, "Invalid Content-Type")
	}
	if charset, ok := contentTypeParams["charset"]; ok && strings.ToLower(charset) != "utf-8" {
		x.SetStatus(w, x.ErrorInvalidRequest, "Unsupported charset. "+
			"Supported charset is UTF-8")
		return
	}

	switch mediaType {
	case "application/json":
		if err := json.Unmarshal(body, &params); err != nil {
			jsonErr := convertJSONError(string(body), err)
			x.SetStatus(w, x.ErrorInvalidRequest, jsonErr.Error())
			return
		}
	case "application/graphql+-", "application/dql", "application/x-www-form-urlencoded":
		// If nothing is specified, it would be marked as x-www-form-urlencoded.
		// In that case, assume DQL.
		params.Query = string(body)
	default:
		x.SetStatus(w, x.ErrorInvalidRequest, "Unsupported Content-Type. "+
			"Supported content types are application/json, application/graphql+-,application/dql")
		return
	}

	ctx := context.WithValue(r.Context(), query.DebugKey, isDebugMode)
	ctx = x.AttachAccessJwt(ctx, r)
	ctx = x.AttachRemoteIP(ctx, r)

	if queryTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, queryTimeout)
		defer cancel()
	}

	req := pb.Request{
		Vars:    params.Variables,
		Query:   params.Query,
		StartTs: startTs,
		Hash:    hash,
	}

	if req.StartTs == 0 {
		// If be is set, run this as a best-effort query.
		isBestEffort, err := parseBool(r, "be")
		if err != nil {
			x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
			return
		}
		if isBestEffort {
			req.BestEffort = true
			req.ReadOnly = true
		}

		// If ro is set, run this as a readonly query.
		isReadOnly, err := parseBool(r, "ro")
		if err != nil {
			x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
			return
		}
		if isReadOnly {
			req.ReadOnly = true
		}
	}

	// Core processing happens here.
	resp, err := edgraph.Query(ctx, &req)
	if err != nil {
		x.SetStatusWithData(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	// Add cost to the header.
	w.Header().Set(x.DgraphCostHeader, fmt.Sprint(resp.Metrics.NumUids["_total"]))

	e := query.Extensions{
		Txn:     resp.Txn,
		Latency: resp.Latency,
		Metrics: resp.Metrics,
	}
	js, err := json.Marshal(e)
	if err != nil {
		x.SetStatusWithData(w, x.Error, err.Error())
		return
	}

	var out bytes.Buffer
	writeEntry := func(key string, js []byte) {
		x.Check2(out.WriteRune('"'))
		x.Check2(out.WriteString(key))
		x.Check2(out.WriteRune('"'))
		x.Check2(out.WriteRune(':'))
		x.Check2(out.Write(js))
	}
	x.Check2(out.WriteRune('{'))
	writeEntry("data", resp.Json)
	x.Check2(out.WriteRune(','))
	writeEntry("extensions", js)
	x.Check2(out.WriteRune('}'))

	if _, err := x.WriteResponse(w, r, out.Bytes()); err != nil {
		// If client crashes before server could write response, writeResponse will error out,
		// Check2 will fatal and shut the server down in such scenario. We don't want that.
		glog.Errorln("Unable to write response: ", err)
	}
}

func alterHandler(w http.ResponseWriter, r *http.Request) {
	if commonHandler(w, r) {
		return
	}

	b := readRequest(w, r)
	if b == nil {
		return
	}

	op := &pb.Operation{}
	if err := jsonpb.UnmarshalString(string(b), op); err != nil {
		op.Schema = string(b)
	}

	runInBackground, err := parseBool(r, "runInBackground")
	if err != nil {
		x.SetStatus(w, x.ErrorInvalidRequest, err.Error())
		return
	}
	op.RunInBackground = runInBackground

	glog.Infof("Got alter request via HTTP from %s\n", r.RemoteAddr)
	fwd := r.Header.Get("X-Forwarded-For")
	if len(fwd) > 0 {
		glog.Infof("The alter request is forwarded by %s\n", fwd)
	}

	// Pass in PoorMan's auth, ACL and IP information if present.
	ctx := x.AttachAuthToken(context.Background(), r)
	ctx = x.AttachAccessJwt(ctx, r)
	ctx = x.AttachRemoteIP(ctx, r)
	if _, err := edgraph.Alter(ctx, op); err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}

	writeSuccessResponse(w, r)
}

func adminSchemaHandler(w http.ResponseWriter, r *http.Request) {
	if commonHandler(w, r) {
		return
	}

	b := readRequest(w, r)
	if b == nil {
		return
	}

	gqlReq := &schema.Request{
		Query: `
		mutation updateGqlSchema($sch: String!) {
			updateGQLSchema(input: {
				set: {
					schema: $sch
				}
			}) {
				gqlSchema {
					id
				}
			}
		}`,
		Variables: map[string]interface{}{"sch": string(b)},
	}

	response := resolveWithAdminServer(gqlReq, r, adminServer)
	if len(response.Errors) > 0 {
		x.SetStatus(w, x.Error, response.Errors.Error())
		return
	}
	if err := admin.LoadSchema(0); err != nil {
		glog.Errorf("LoadSchema: Unable to load schema. Error: %v", err)
		x.SetStatus(w, x.Error,
			fmt.Sprintf("LoadSchema: Stored schema, but unable to load. Error: %v", err))
		return
	}
	writeSuccessResponse(w, r)
}

func lambdaUpdateHandler(w http.ResponseWriter, r *http.Request) {
	if commonHandler(w, r) {
		return
	}

	data := readRequest(w, r)
	if len(data) == 0 {
		return
	}
	scr := base64.StdEncoding.EncodeToString(data)

	gqlReq := &schema.Request{
		Query: `
mutation updateLambda($scr: String!) {
updateLambdaScript(input: {set: {script: $scr }}) {
		lambdaScript { script }
	}}`,
		Variables: map[string]interface{}{"scr": scr},
	}

	resp := resolveWithAdminServer(gqlReq, r, adminServer)
	if len(resp.Errors) > 0 {
		x.SetStatus(w, x.Error, resp.Errors.Error())
		return
	}

	type script struct {
		Script string `json:"script"`
	}
	type uls struct {
		LambdaScript script `json:"lambdaScript"`
	}
	type D struct {
		Uls uls `json:"updateLambdaScript"`
	}

	var d D
	if err := json.Unmarshal(resp.Data.Bytes(), &d); err != nil {
		x.SetStatus(w, x.Error, fmt.Sprintf("Error while unmarshal of response: %s", err))
		return
	}
	if got := d.Uls.LambdaScript.Script; got != scr {
		glog.Errorf("Lambda Update Failed. Wanted:\n%s\nGot:\n%s\n", scr, got)
		x.SetStatus(w, x.Error, fmt.Sprintf("Script doesn't match. Update Failed."))
		return
	}
	if err := admin.LoadSchema(0); err != nil {
		glog.Errorf("LoadSchema: Unable to load lambda script. Error: %v", err)
		x.SetStatus(w, x.Error,
			fmt.Sprintf("LoadSchema: Stored lambda script, but unable to load. Error: %v", err))
		return
	}
	writeSuccessResponse(w, r)
}

func graphqlProbeHandler(gqlHealthStore *admin.GraphQLHealthStore, globalEpoch map[uint64]*uint64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		x.AddCorsHeaders(w)
		w.Header().Set("Content-Type", "application/json")
		// lazy load the schema so that just by making a probe request,
		// one can boot up GraphQL for their namespace
		namespace := x.ExtractNamespaceHTTP(r)
		if err := admin.LoadSchema(namespace); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			x.Check2(w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err))))
			return
		}

		healthStatus := gqlHealthStore.GetHealth()
		httpStatusCode := http.StatusOK
		if !healthStatus.Healthy {
			httpStatusCode = http.StatusServiceUnavailable
		}
		w.WriteHeader(httpStatusCode)
		e := globalEpoch[namespace]
		var counter uint64
		if e != nil {
			counter = atomic.LoadUint64(e)
		}
		x.Check2(w.Write([]byte(fmt.Sprintf(`{"status":"%s","schemaUpdateCounter":%d}`,
			healthStatus.StatusMsg, counter))))
	})
}

func resolveWithAdminServer(gqlReq *schema.Request, r *http.Request,
	adminServer *admin.GqlHandler) *schema.Response {
	md := metadata.New(nil)
	ctx := metadata.NewIncomingContext(context.Background(), md)
	ctx = x.AttachAccessJwt(ctx, r)
	ctx = x.AttachRemoteIP(ctx, r)
	ctx = x.AttachAuthToken(ctx, r)
	ctx = x.AttachJWTNamespace(ctx)

	return adminServer.ResolveWithNs(ctx, x.GalaxyNamespace, gqlReq)
}

func writeSuccessResponse(w http.ResponseWriter, r *http.Request) {
	res := map[string]interface{}{}
	data := map[string]interface{}{}
	data["code"] = x.Success
	data["message"] = "Done"
	res["data"] = data

	js, err := json.Marshal(res)
	if err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}

	_, _ = x.WriteResponse(w, r, js)
}

// skipJSONUnmarshal stores the raw bytes as is while JSON unmarshaling.
type skipJSONUnmarshal struct {
	bs []byte
}

func (sju *skipJSONUnmarshal) UnmarshalJSON(bs []byte) error {
	sju.bs = bs
	return nil
}

// convertJSONError adds line and character information to the JSON error.
// Idea taken from: https://bit.ly/2moFIVS
func convertJSONError(input string, err error) error {
	if err == nil {
		return nil
	}

	if jsonError, ok := err.(*json.SyntaxError); ok {
		line, character, lcErr := jsonLineAndChar(input, int(jsonError.Offset))
		if lcErr != nil {
			return err
		}
		return errors.Errorf("Error parsing JSON at line %d, character %d: %v\n", line, character,
			jsonError.Error())
	}

	if jsonError, ok := err.(*json.UnmarshalTypeError); ok {
		line, character, lcErr := jsonLineAndChar(input, int(jsonError.Offset))
		if lcErr != nil {
			return err
		}
		return errors.Errorf("Error parsing JSON at line %d, character %d: %v\n", line, character,
			jsonError.Error())
	}

	return err
}

func jsonLineAndChar(input string, offset int) (line int, character int, err error) {
	lf := rune(0x0A)

	if offset > len(input) || offset < 0 {
		return 0, 0, errors.Errorf("Couldn't find offset %d within the input.", offset)
	}

	line = 1
	for i, b := range input {
		if b == lf {
			line++
			character = 0
		}
		character++
		if i == offset {
			break
		}
	}

	return line, character, nil
}
