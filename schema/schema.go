// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package schema

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"

	"github.com/outcaste-io/badger/v3"
	badgerpb "github.com/outcaste-io/badger/v3/pb"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/tok"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"
)

var (
	pstate *state
	pstore *badger.DB
)

// We maintain two schemas for a predicate if a background task is building indexes
// for that predicate. Now, we need to use the new schema for mutations whereas
// a query schema for queries. While calling functions in this package, we need
// to set the context correctly as to which schema should be returned.
// Query schema is defined as (old schema - tokenizers to drop based on new schema).
type contextKey int

const (
	isWrite contextKey = iota
)

// GetWriteContext returns a context that sets the schema context for writing.
func GetWriteContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, isWrite, true)
}

func (s *state) init() {
	s.predicate = make(map[string]*pb.SchemaUpdate)
	s.elog = trace.NewEventLog("Dgraph", "Schema")
	s.mutSchema = make(map[string]*pb.SchemaUpdate)
}

type state struct {
	sync.RWMutex
	// Map containing predicate to type information.
	predicate map[string]*pb.SchemaUpdate
	elog      trace.EventLog
	// mutSchema holds the schema update that is being applied in the background.
	mutSchema map[string]*pb.SchemaUpdate
}

// State returns the struct holding the current schema.
func State() *state {
	return pstate
}

func (s *state) DeleteAll() {
	s.Lock()
	defer s.Unlock()

	for pred := range s.predicate {
		delete(s.predicate, pred)
	}

	for pred := range s.mutSchema {
		delete(s.mutSchema, pred)
	}
}

// Delete updates the schema in memory and disk
func (s *state) Delete(attr string, ts uint64) error {
	s.Lock()
	defer s.Unlock()

	glog.Infof("Deleting schema for predicate: [%s]", attr)
	txn := pstore.NewTransactionAt(ts, true)
	if err := txn.Delete(x.SchemaKey(attr)); err != nil {
		return err
	}
	// Delete is called rarely so sync write should be fine.
	if err := txn.CommitAt(ts, nil); err != nil {
		return err
	}

	delete(s.predicate, attr)
	delete(s.mutSchema, attr)
	return nil
}

// Namespaces returns the active namespaces based on the current types.
func (s *state) Namespaces() map[uint64]struct{} {
	if s == nil {
		return nil
	}

	s.RLock()
	defer s.RUnlock()

	// TODO: Find another way of figuring out namespaces.
	return nil
}

// DeletePredsForNs deletes the predicate information for the namespace from the schema.
func (s *state) DeletePredsForNs(delNs uint64) {
	if s == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	for pred := range s.predicate {
		ns := x.ParseNamespace(pred)
		if ns == delNs {
			delete(s.predicate, pred)
			delete(s.mutSchema, pred)
		}
	}
}

func logUpdate(schema *pb.SchemaUpdate, pred string) string {
	if schema == nil {
		return ""
	}

	typ := types.TypeID(schema.ValueType).String()
	if schema.List {
		typ = fmt.Sprintf("[%s]", typ)
	}
	return fmt.Sprintf("Setting schema for attr %s: %v, tokenizer: %v, directive: %v, count: %v\n",
		pred, typ, schema.Tokenizer, schema.Directive, schema.Count)
}

// Set sets the schema for the given predicate in memory.
// Schema mutations must flow through the update function, which are synced to the db.
func (s *state) Set(pred string, schema *pb.SchemaUpdate) {
	if schema == nil {
		return
	}

	s.Lock()
	defer s.Unlock()
	s.predicate[pred] = schema
	s.elog.Printf(logUpdate(schema, pred))
}

// SetMutSchema sets the mutation schema for the given predicate.
func (s *state) SetMutSchema(pred string, schema *pb.SchemaUpdate) {
	s.Lock()
	defer s.Unlock()
	s.mutSchema[pred] = schema
}

// DeleteMutSchema deletes the schema for given predicate from mutSchema.
func (s *state) DeleteMutSchema(pred string) {
	s.Lock()
	defer s.Unlock()
	delete(s.mutSchema, pred)
}

// GetIndexingPredicates returns the list of predicates for which we are building indexes.
func GetIndexingPredicates() []string {
	s := State()
	s.Lock()
	defer s.Unlock()
	if len(s.mutSchema) == 0 {
		return nil
	}

	ps := make([]string, 0, len(s.mutSchema))
	for p := range s.mutSchema {
		ps = append(ps, p)
	}
	return ps
}

// Get gets the schema for the given predicate.
func (s *state) Get(ctx context.Context, pred string) (pb.SchemaUpdate, bool) {
	isWrite, _ := ctx.Value(isWrite).(bool)
	s.RLock()
	defer s.RUnlock()
	// If this is write context, mutSchema will have the updated schema.
	// If mutSchema doesn't have the predicate key, we use the schema from s.predicate.
	if isWrite {
		if schema, ok := s.mutSchema[pred]; ok {
			return *schema, true
		}
	}

	schema, ok := s.predicate[pred]
	if !ok {
		return pb.SchemaUpdate{}, false
	}
	return *schema, true
}

// TypeOf returns the schema type of predicate
func (s *state) TypeOf(pred string) (types.TypeID, error) {
	s.RLock()
	defer s.RUnlock()
	if schema, ok := s.predicate[pred]; ok {
		return types.TypeID(schema.ValueType), nil
	}
	err := errors.Errorf("Schema not defined for predicate: %q. Available: %+v",
		pred, s.predicate)
	return types.TypeUndefined, err
}

// IsIndexed returns whether the predicate is indexed or not
func (s *state) IsIndexed(ctx context.Context, pred string) bool {
	isWrite, _ := ctx.Value(isWrite).(bool)
	s.RLock()
	defer s.RUnlock()
	if isWrite {
		// TODO(Aman): we could return the query schema if it is a delete.
		if schema, ok := s.mutSchema[pred]; ok && len(schema.Tokenizer) > 0 {
			return true
		}
	}

	if schema, ok := s.predicate[pred]; ok {
		return len(schema.Tokenizer) > 0
	}

	return false
}

// Predicates returns the list of predicates for given group
func (s *state) Predicates() []string {
	if s == nil {
		return nil
	}

	s.RLock()
	defer s.RUnlock()
	var out []string
	for k := range s.predicate {
		out = append(out, k)
	}
	return out
}

func (s *state) PredicatesFor(typeName ...string) []string {
	if s == nil {
		return nil
	}

	types := make(map[string]struct{})
	for _, tn := range typeName {
		types[tn] = struct{}{}
	}

	s.RLock()
	defer s.RUnlock()

	// All predicates are of form: <typename>.<fieldname> . So, we can just
	// split the predicate names by dot and check if the type name matches.
	var out []string
	for k := range s.predicate {
		tn := strings.SplitN(k, ".", 2)[0]
		if _, has := types[tn]; has {
			out = append(out, k)
		}
	}
	return out
}

// Tokenizer returns the tokenizer for given predicate
func (s *state) Tokenizer(ctx context.Context, pred string) []tok.Tokenizer {
	isWrite, _ := ctx.Value(isWrite).(bool)
	s.RLock()
	defer s.RUnlock()
	var su *pb.SchemaUpdate
	if isWrite {
		if schema, ok := s.mutSchema[pred]; ok {
			su = schema
		}
	}
	if su == nil {
		if schema, ok := s.predicate[pred]; ok {
			su = schema
		}
	}
	if su == nil {
		// This may happen when some query that needs indexing over this predicate is executing
		// while the predicate is dropped from the state (using drop operation).
		glog.Errorf("Schema state not found for %s.", pred)
		return nil
	}
	tokenizers := make([]tok.Tokenizer, 0, len(su.Tokenizer))
	for _, it := range su.Tokenizer {
		t, found := tok.GetTokenizer(it)
		x.AssertTruef(found, "Invalid tokenizer %s", it)
		tokenizers = append(tokenizers, t)
	}
	return tokenizers
}

// TokenizerNames returns the tokenizer names for given predicate
func (s *state) TokenizerNames(ctx context.Context, pred string) []string {
	var names []string
	tokenizers := s.Tokenizer(ctx, pred)
	for _, t := range tokenizers {
		names = append(names, t.Name())
	}
	return names
}

// HasTokenizer is a convenience func that checks if a given tokenizer is found in pred.
// Returns true if found, else false.
func (s *state) HasTokenizer(ctx context.Context, id byte, pred string) bool {
	for _, t := range s.Tokenizer(ctx, pred) {
		if t.Identifier() == id {
			return true
		}
	}
	return false
}

// HasCount returns whether we want to mantain a count index for the given predicate or not.
func (s *state) HasCount(ctx context.Context, pred string) bool {
	isWrite, _ := ctx.Value(isWrite).(bool)
	s.RLock()
	defer s.RUnlock()
	if isWrite {
		if schema, ok := s.mutSchema[pred]; ok && schema.Count {
			return true
		}
	}
	if schema, ok := s.predicate[pred]; ok {
		return schema.Count
	}
	return false
}

// IsList returns whether the predicate is of list type.
func (s *state) IsList(pred string) bool {
	s.RLock()
	defer s.RUnlock()
	if schema, ok := s.predicate[pred]; ok {
		return schema.List
	}
	return false
}

func (s *state) HasUpsert(pred string) bool {
	s.RLock()
	defer s.RUnlock()
	if schema, ok := s.predicate[pred]; ok {
		return schema.Upsert
	}
	return false
}

func (s *state) HasLang(pred string) bool {
	return false
}

// IndexingInProgress checks whether indexing is going on for a given predicate.
func (s *state) IndexingInProgress() bool {
	s.RLock()
	defer s.RUnlock()
	return len(s.mutSchema) > 0
}

// Init resets the schema state, setting the underlying DB to the given pointer.
func Init(ps *badger.DB) {
	pstore = ps
	reset()
}

// Load reads the latest schema for the given predicate from the DB.
func Load(predicate string) error {
	if len(predicate) == 0 {
		return errors.Errorf("Empty predicate")
	}
	delete(State().mutSchema, predicate)
	key := x.SchemaKey(predicate)
	txn := pstore.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound || err == badger.ErrBannedKey {
		return nil
	}
	if err != nil {
		return err
	}
	var s pb.SchemaUpdate
	err = item.Value(func(val []byte) error {
		x.Check(s.Unmarshal(val))
		return nil
	})
	if err != nil {
		return err
	}
	State().Set(predicate, &s)
	State().elog.Printf(logUpdate(&s, predicate))
	glog.Infoln(logUpdate(&s, predicate))
	return nil
}

// LoadFromDb reads schema information from db and stores it in memory
func LoadFromDb() error {
	// Reset the state because with the introduction of incremental restore, it can't be assumed
	// that the state would be empty before loading the schema from the DB as we don't do drop all
	// in case of incremental restores.
	State().DeleteAll()
	return loadFromDB(loadSchema)
}

const (
	loadSchema int = iota
)

// loadFromDb iterates through the DB and loads all the stored schema updates.
func loadFromDB(loadType int) error {
	stream := pstore.NewStreamAt(math.MaxUint64)

	switch loadType {
	case loadSchema:
		stream.Prefix = x.SchemaPrefix()
		stream.LogPrefix = "LoadFromDb Schema"
	default:
		glog.Fatalf("Invalid load type")
	}

	stream.KeyToList = func(key []byte, itr *badger.Iterator) (*badgerpb.KVList, error) {
		item := itr.Item()
		pk, err := x.Parse(key)
		if err != nil {
			glog.Errorf("Error while parsing key %s: %v", hex.Dump(key), err)
			return nil, nil
		}
		if len(pk.Attr) == 0 {
			glog.Warningf("Empty Attribute: %+v for Key: %x\n", pk, key)
			return nil, nil
		}

		switch loadType {
		case loadSchema:
			var s pb.SchemaUpdate
			err := item.Value(func(val []byte) error {
				if len(val) == 0 {
					s = pb.SchemaUpdate{Predicate: pk.Attr, ValueType: int32(types.TypeDefault)}
				}
				x.Checkf(s.Unmarshal(val), "Error while loading schema from db")
				glog.Infof("Setting schema: %+v\n", s)
				State().Set(pk.Attr, &s)
				return nil
			})
			return nil, err
		}
		glog.Fatalf("Invalid load type")
		return nil, errors.New("shouldn't reach here")
	}
	return stream.Orchestrate(context.Background())
}

// InitialSchema returns the schema updates to insert at the beginning of
// Dgraph's execution. It looks at the worker options to determine which
// attributes to insert.
func InitialSchema(namespace uint64) []*pb.SchemaUpdate {
	return initialSchemaInternal(namespace, false)
}

// CompleteInitialSchema returns all the schema updates regardless of the worker
// options. This is useful in situations where the worker options are not known
// in advance and it's better to create all the reserved predicates and remove
// them later than miss some of them. An example of such situation is during bulk
// loading.
func CompleteInitialSchema(namespace uint64) []*pb.SchemaUpdate {
	return initialSchemaInternal(namespace, true)
}

func initialSchemaInternal(namespace uint64, all bool) []*pb.SchemaUpdate {
	var initialSchema []*pb.SchemaUpdate

	initialSchema = append(initialSchema,
		&pb.SchemaUpdate{
			Predicate: "dgraph.type",
			ValueType: types.TypeString.Int(),
			Directive: pb.SchemaUpdate_INDEX,
			Tokenizer: []string{"exact"},
			List:      true,
		}, &pb.SchemaUpdate{
			Predicate: "dgraph.drop.op",
			ValueType: types.TypeString.Int(),
		}, &pb.SchemaUpdate{
			Predicate: "dgraph.graphql.schema",
			ValueType: types.TypeString.Int(),
		}, &pb.SchemaUpdate{
			Predicate: "dgraph.graphql.xid",
			ValueType: types.TypeString.Int(),
			Directive: pb.SchemaUpdate_INDEX,
			Tokenizer: []string{"exact"},
			Upsert:    true,
		}, &pb.SchemaUpdate{
			Predicate: "dgraph.graphql.p_query",
			ValueType: types.TypeString.Int(),
			Directive: pb.SchemaUpdate_INDEX,
			Tokenizer: []string{"sha256"},
		})

	if all || x.WorkerConfig.AclEnabled {
		// propose the schema update for acl predicates
		initialSchema = append(initialSchema, []*pb.SchemaUpdate{
			{
				Predicate: "dgraph.xid",
				ValueType: types.TypeString.Int(),
				Directive: pb.SchemaUpdate_INDEX,
				Upsert:    true,
				Tokenizer: []string{"exact"},
			},
			{
				Predicate: "dgraph.password",
				ValueType: types.TypePassword.Int(),
			},
			{
				Predicate: "dgraph.user.group",
				// Note: This was using a reverse directive. Instead, we should
				// be handling this via GraphQL.
				ValueType: types.TypeUid.Int(),
				List:      true,
			},
			{
				Predicate: "dgraph.acl.rule",
				ValueType: types.TypeUid.Int(),
				List:      true,
			},
			{
				Predicate: "dgraph.rule.predicate",
				ValueType: types.TypeString.Int(),
				Directive: pb.SchemaUpdate_INDEX,
				Tokenizer: []string{"exact"},
				Upsert:    true, // Not really sure if this will work.
			},
			{
				Predicate: "dgraph.rule.permission",
				ValueType: types.TypeInt64.Int(),
			},
		}...)
	}
	for _, sch := range initialSchema {
		sch.Predicate = x.NamespaceAttr(namespace, sch.Predicate)
	}
	return initialSchema
}

// IsPreDefPredChanged returns true if the initial update for the pre-defined
// predicate is different than the passed update.
// If the passed update is not a pre-defined predicate then it just returns false.
func IsPreDefPredChanged(update *pb.SchemaUpdate) bool {
	// Return false for non-pre-defined predicates.
	if !x.IsPreDefinedPredicate(update.Predicate) {
		return false
	}

	initialSchema := CompleteInitialSchema(x.ParseNamespace(update.Predicate))
	for _, original := range initialSchema {
		if original.Predicate != update.Predicate {
			continue
		}
		return !proto.Equal(original, update)
	}
	return true
}

func reset() {
	pstate = new(state)
	pstate.init()
}
