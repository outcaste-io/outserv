// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package schema

import (
	"math"
	"strconv"
	"strings"

	"github.com/outcaste-io/outserv/lex"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/tok"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"

	"github.com/pkg/errors"
)

// ParseBytes parses the byte array which holds the schema. We will reset
// all the globals.
// Overwrites schema blindly - called only during initilization in testing
func ParseBytes(s []byte, gid uint32) (rerr error) {
	if pstate == nil {
		reset()
	}
	pstate.DeleteAll()
	result, err := Parse(string(s))
	if err != nil {
		return err
	}

	for _, update := range result.Preds {
		State().Set(update.Predicate, update)
	}
	return nil
}

func parseDirective(it *lex.ItemIterator, schema *pb.SchemaUpdate, t types.TypeID) error {
	it.Next()
	next := it.Item()
	if next.Typ != itemText {
		return next.Errorf("Missing directive name")
	}
	switch next.Val {
	case "index":
		tokenizer, err := parseIndexDirective(it, schema.Predicate, t)
		if err != nil {
			return err
		}
		schema.Directive = pb.SchemaUpdate_INDEX
		schema.Tokenizer = tokenizer
	case "count":
		schema.Count = true
	case "upsert":
		schema.Upsert = true
	default:
		return next.Errorf("Invalid index specification")
	}
	it.Next()

	return nil
}

func parseScalarPair(it *lex.ItemIterator, predicate string, ns uint64) (*pb.SchemaUpdate, error) {
	it.Next()
	next := it.Item()
	switch {
	// This check might seem redundant but it's necessary. We have two possibilities,
	//   1) that the schema is of form: name@en: string .
	//
	//   2) or this alternate form: <name@en>: string .
	//
	// The itemAt test invalidates 1) and string.Contains() tests for 2). We don't allow
	// '@' in predicate names, so both forms are disallowed. Handling them here avoids
	// messing with the lexer and IRI values.
	case next.Typ == itemAt || strings.Contains(predicate, "@"):
		return nil, next.Errorf("Invalid '@' in name")
	case next.Typ != itemColon:
		return nil, next.Errorf("Missing colon")
	case !it.Next():
		return nil, next.Errorf("Invalid ending while trying to parse schema.")
	}
	next = it.Item()
	schema := &pb.SchemaUpdate{Predicate: x.NamespaceAttr(ns, predicate)}
	// Could be list type.
	if next.Typ == itemLeftSquare {
		schema.List = true
		if !it.Next() {
			return nil, next.Errorf("Invalid ending while trying to parse schema.")
		}
		next = it.Item()
	}

	if next.Typ != itemText {
		return nil, next.Errorf("Missing Type")
	}
	typ := strings.ToLower(next.Val)
	// We ignore the case for types.
	t, ok := types.TypeForName(typ)
	if !ok {
		return nil, next.Errorf("Undefined Type")
	}
	if schema.List {
		if uint32(t) == uint32(types.TypePassword) || uint32(t) == uint32(types.TypeBool) {
			return nil, next.Errorf("Unsupported type for list: [%s].", types.TypeID(t))
		}
	}
	schema.ValueType = t.Int()

	// Check for index / reverse.
	it.Next()
	next = it.Item()
	if schema.List {
		if next.Typ != itemRightSquare {
			return nil, next.Errorf("Unclosed [ while parsing schema for: %s", predicate)
		}
		if !it.Next() {
			return nil, next.Errorf("Invalid ending")
		}
		next = it.Item()
	}

	for {
		if next.Typ != itemAt {
			break
		}
		if err := parseDirective(it, schema, t); err != nil {
			return nil, err
		}
		next = it.Item()
	}

	if next.Typ != itemDot {
		return nil, next.Errorf("Invalid ending")
	}
	it.Next()
	next = it.Item()
	if next.Typ == lex.ItemEOF {
		it.Prev()
		return schema, nil
	}
	if next.Typ != itemNewLine {
		return nil, next.Errorf("Invalid ending")
	}
	return schema, nil
}

// parseIndexDirective works on "@index" or "@index(customtokenizer)".
func parseIndexDirective(it *lex.ItemIterator, predicate string,
	typ types.TypeID) ([]string, error) {
	var tokenizers []string
	var seen = make(map[string]bool)
	var seenSortableTok bool

	if typ == types.TypeUid || typ == types.TypeDefault || typ == types.TypePassword {
		return tokenizers, it.Item().Errorf("Indexing not allowed on predicate %s of type %s",
			predicate, typ)
	}
	if !it.Next() {
		// Nothing to read.
		return []string{}, it.Item().Errorf("Invalid ending.")
	}
	next := it.Item()
	if next.Typ != itemLeftRound {
		it.Prev() // Backup.
		return []string{}, it.Item().Errorf("Require type of tokenizer for pred: %s for indexing.",
			predicate)
	}

	expectArg := true
	// Look for tokenizers.
	for {
		it.Next()
		next = it.Item()
		if next.Typ == itemRightRound {
			break
		}
		if next.Typ == itemComma {
			if expectArg {
				return nil, next.Errorf("Expected a tokenizer but got comma")
			}
			expectArg = true
			continue
		}
		if next.Typ != itemText {
			return tokenizers, next.Errorf("Expected directive arg but got: %v", next.Val)
		}
		if !expectArg {
			return tokenizers, next.Errorf("Expected a comma but got: %v", next)
		}
		// Look for custom tokenizer.
		tokenizer, has := tok.GetTokenizer(strings.ToLower(next.Val))
		if !has {
			return tokenizers, next.Errorf("Invalid tokenizer %s", next.Val)
		}
		tokenizerType, ok := types.TypeForName(tokenizer.Type())
		x.AssertTrue(ok) // Type is validated during tokenizer loading.
		if tokenizerType != typ {
			return tokenizers,
				next.Errorf("Tokenizer: %s isn't valid for predicate: %s of type: %s",
					tokenizer.Name(), x.ParseAttr(predicate), typ)
		}
		if _, found := seen[tokenizer.Name()]; found {
			return tokenizers, next.Errorf("Duplicate tokenizers defined for pred %v",
				predicate)
		}
		if tokenizer.IsSortable() {
			if seenSortableTok {
				return nil, next.Errorf("More than one sortable index encountered for: %v",
					predicate)
			}
			seenSortableTok = true
		}
		tokenizers = append(tokenizers, tokenizer.Name())
		seen[tokenizer.Name()] = true
		expectArg = false
	}
	return tokenizers, nil
}

// resolveTokenizers resolves default tokenizers and verifies tokenizers definitions.
func resolveTokenizers(updates []*pb.SchemaUpdate) error {
	for _, schema := range updates {
		typ := types.TypeID(schema.ValueType)

		if (typ == types.TypeUid || typ == types.TypeDefault || typ == types.TypePassword) &&
			schema.Directive == pb.SchemaUpdate_INDEX {
			return errors.Errorf("Indexing not allowed on predicate %s of type %s",
				x.ParseAttr(schema.Predicate), typ)
		}

		if typ == types.TypeUid {
			continue
		}

		if len(schema.Tokenizer) == 0 && schema.Directive == pb.SchemaUpdate_INDEX {
			return errors.Errorf("Require type of tokenizer for pred: %s of type: %s for indexing.",
				schema.Predicate, typ)
		} else if len(schema.Tokenizer) > 0 && schema.Directive != pb.SchemaUpdate_INDEX {
			return errors.Errorf("Tokenizers present without indexing on attr %s", x.ParseAttr(schema.Predicate))
		}
		// check for valid tokeniser types and duplicates
		var seen = make(map[string]bool)
		var seenSortableTok bool
		for _, t := range schema.Tokenizer {
			tokenizer, has := tok.GetTokenizer(t)
			if !has {
				return errors.Errorf("Invalid tokenizer %s", t)
			}
			tokenizerType, ok := types.TypeForName(tokenizer.Type())
			x.AssertTrue(ok) // Type is validated during tokenizer loading.
			if tokenizerType != typ {
				return errors.Errorf("Tokenizer: %s isn't valid for predicate: %s of type: %s",
					tokenizer.Name(), x.ParseAttr(schema.Predicate), typ)
			}
			if _, ok := seen[tokenizer.Name()]; !ok {
				seen[tokenizer.Name()] = true
			} else {
				return errors.Errorf("Duplicate tokenizers present for attr %s",
					x.ParseAttr(schema.Predicate))
			}
			if tokenizer.IsSortable() {
				if seenSortableTok {
					return errors.Errorf("More than one sortable index encountered for: %v",
						schema.Predicate)
				}
				seenSortableTok = true
			}
		}
	}
	return nil
}

func parseNamespace(it *lex.ItemIterator) (uint64, error) {
	nextItems, err := it.Peek(2)
	if err != nil {
		return 0, errors.Errorf("Unable to peek: %v", err)
	}
	if nextItems[0].Typ != itemNumber || nextItems[1].Typ != itemRightSquare {
		return 0, errors.Errorf("Typed oes not match the expected")
	}
	ns, err := strconv.ParseUint(nextItems[0].Val, 0, 64)
	if err != nil {
		return 0, err
	}
	it.Next()
	it.Next()
	// We have parsed the namespace. Now move to the next item.
	if !it.Next() {
		return 0, errors.Errorf("No schema found after namespace. Got: %v", nextItems[0])
	}
	return uint64(ns), nil
}

// ParsedSchema represents the parsed schema and type updates.
type ParsedSchema struct {
	Preds []*pb.SchemaUpdate
}

// parse parses a schema string and returns the schema representation for it.
// If namespace == math.MaxUint64, then it preserves the namespace. Else it forces the passed
// namespace on schema/types.

// Example schema:
// [ns1] name: string .
// [ns2] age: string .
// parse(schema, 0) --> All the schema fields go to namespace 0.
// parse(schema, x) --> All the schema fields go to namespace x.
// parse(schema, math.MaxUint64) --> name (ns1), age(ns2) // Preserve the namespace
func parse(s string, namespace uint64) (*ParsedSchema, error) {
	var result ParsedSchema

	var l lex.Lexer
	l.Reset(s)
	l.Run(lexText)
	if err := l.ValidateResult(); err != nil {
		return nil, err
	}

	parseSchema := func(item lex.Item, it *lex.ItemIterator, ns uint64) error {
		schema, err := parseScalarPair(it, item.Val, ns)
		if err != nil {
			return err
		}
		result.Preds = append(result.Preds, schema)
		return nil
	}

	it := l.NewIterator()
	for it.Next() {
		item := it.Item()
		switch item.Typ {
		case lex.ItemEOF:
			if err := resolveTokenizers(result.Preds); err != nil {
				return nil, errors.Wrapf(err, "failed to enrich schema")
			}
			return &result, nil

		case itemText:
			// For schema which does not contain the namespace information, use the default
			// namespace, if namespace has to be preserved. Else, use the passed namespace.
			ns := x.GalaxyNamespace
			if namespace != math.MaxUint64 {
				ns = uint64(namespace)
			}
			if err := parseSchema(item, it, ns); err != nil {
				return nil, err
			}

		case itemLeftSquare:
			// We expect a namespace.
			ns, err := parseNamespace(it)
			if err != nil {
				return nil, errors.Wrapf(err, "While parsing namespace:")
			}
			if namespace != math.MaxUint64 {
				// Use the passed namespace, if we don't want to preserve the namespace.
				ns = uint64(namespace)
			}
			// We have already called next in parseNamespace.
			item := it.Item()
			if err := parseSchema(item, it, ns); err != nil {
				return nil, err
			}

		case itemNewLine:
			// pass empty line

		default:
			return nil, it.Item().Errorf("Unexpected token: %v while parsing schema", item)
		}
	}
	return nil, errors.Errorf("Shouldn't reach here")
}

// Parse parses the schema with namespace preserved. For the types/predicates for which the
// namespace is not specified, it uses default.
func Parse(s string) (*ParsedSchema, error) {
	return parse(s, math.MaxUint64)
}

// ParseWithNamespace parses the schema and forces the given namespace on each of the
// type/predicate.
func ParseWithNamespace(s string, namespace uint64) (*ParsedSchema, error) {
	return parse(s, namespace)
}
