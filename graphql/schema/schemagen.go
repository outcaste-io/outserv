// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/outcaste-io/gqlparser/v2/ast"
	"github.com/outcaste-io/gqlparser/v2/gqlerror"
	"github.com/outcaste-io/gqlparser/v2/parser"
	"github.com/outcaste-io/gqlparser/v2/validator"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
)

// A Handler can produce valid GraphQL and Dgraph schemas given an input of
// types and relationships
type Handler struct {
	input          string
	originalDefs   []string
	completeSchema *ast.Schema
	dgraphSchema   string
}

// FromString builds a GraphQL Schema from input string, or returns any parsing
// or validation errors.
func FromString(schema string, ns uint64) (*Schema, error) {
	// validator.Prelude includes a bunch of predefined types which help with schema introspection
	// queries, hence we include it as part of the schema.
	doc, gqlErr := parser.ParseSchemas(validator.Prelude, &ast.Source{Input: schema})
	if gqlErr != nil {
		return nil, errors.Wrap(gqlErr, "while parsing GraphQL schema")
	}

	gqlSchema, gqlErr := validator.ValidateSchemaDocument(doc)
	if gqlErr != nil {
		return nil, errors.Wrap(gqlErr, "while validating GraphQL schema")
	}

	return AsSchema(gqlSchema, ns)
}

func (s *Handler) GQLSchema() string {
	return Stringify(s.completeSchema, s.originalDefs, false)
}

func (s *Handler) DGSchema() string {
	return s.dgraphSchema
}

// TODO(mrjn): Understand this better and remove Apollo Federation stuff.
//
// GQLSchemaWithoutApolloExtras return GraphQL schema string
// excluding Apollo extras definitions and Apollo Queries and
// some directives which are not exposed to the Apollo Gateway
// as they are failing in the schema validation which is a bug
// in their library. See here:
// https://github.com/apollographql/apollo-server/issues/3655
func (s *Handler) GQLSchemaWithoutApolloExtras() string {
	typeMapCopy := make(map[string]*ast.Definition)
	for typ, defn := range s.completeSchema.Types {
		// Exclude "union _Entity = ..." definition from types
		if typ == "_Entity" {
			continue
		}
		fldListCopy := make(ast.FieldList, 0)
		for _, fld := range defn.Fields {
			fldDirectiveListCopy := make(ast.DirectiveList, 0)
			for _, dir := range fld.Directives {
				// Drop "@custom" directive from the field's definition.
				if dir.Name == "custom" {
					continue
				}
				fldDirectiveListCopy = append(fldDirectiveListCopy, dir)
			}
			newFld := &ast.FieldDefinition{
				Name:         fld.Name,
				Arguments:    fld.Arguments,
				DefaultValue: fld.DefaultValue,
				Type:         fld.Type,
				Directives:   fldDirectiveListCopy,
				Position:     fld.Position,
			}
			fldListCopy = append(fldListCopy, newFld)
		}

		directiveListCopy := make(ast.DirectiveList, 0)
		for _, dir := range defn.Directives {
			// Drop @generate and @auth directive from the Type Definition.
			if dir.Name == "generate" || dir.Name == "auth" {
				continue
			}
			directiveListCopy = append(directiveListCopy, dir)
		}
		typeMapCopy[typ] = &ast.Definition{
			Kind:       defn.Kind,
			Name:       defn.Name,
			Directives: directiveListCopy,
			Fields:     fldListCopy,
			BuiltIn:    defn.BuiltIn,
			EnumValues: defn.EnumValues,
		}
	}
	queryList := make(ast.FieldList, 0)
	for _, qry := range s.completeSchema.Query.Fields {
		// Drop Apollo Queries from the List of Queries.
		if qry.Name == "_entities" || qry.Name == "_service" {
			continue
		}
		qryDirectiveListCopy := make(ast.DirectiveList, 0)
		for _, dir := range qry.Directives {
			// Drop @custom directive from the Queries.
			if dir.Name == "custom" {
				continue
			}
			qryDirectiveListCopy = append(qryDirectiveListCopy, dir)
		}
		queryList = append(queryList, &ast.FieldDefinition{
			Name:       qry.Name,
			Arguments:  qry.Arguments,
			Type:       qry.Type,
			Directives: qryDirectiveListCopy,
			Position:   qry.Position,
		})
	}

	if typeMapCopy["Query"] != nil {
		typeMapCopy["Query"].Fields = queryList
	}

	queryDefn := &ast.Definition{
		Kind:   ast.Object,
		Name:   "Query",
		Fields: queryList,
	}
	astSchemaCopy := &ast.Schema{
		Query:         queryDefn,
		Mutation:      s.completeSchema.Mutation,
		Subscription:  s.completeSchema.Subscription,
		Types:         typeMapCopy,
		Directives:    s.completeSchema.Directives,
		PossibleTypes: s.completeSchema.PossibleTypes,
		Implements:    s.completeSchema.Implements,
	}
	return Stringify(astSchemaCopy, s.originalDefs, true)
}

// NewHandler processes the input schema. If there are no errors, it returns
// a valid Handler, otherwise it returns nil and an error.
func NewHandler(input string) (*Handler, error) {
	if input == "" {
		return nil, gqlerror.Errorf("No schema specified")
	}

	// The input schema contains just what's required to describe the types,
	// relationships and searchability - but that's not enough to define a
	// valid GraphQL schema: e.g. we allow an input schema file like
	//
	// type T {
	//   f: Int @search
	// }
	//
	// But, that's not valid GraphQL unless there's also definitions of scalars
	// (Int, String, etc) and definitions of the directives (@search, etc).
	// We don't want to make the user have those in their file and then we have
	// to check that they've made the right definitions, etc, etc.
	//
	// So we parse the original input of just types and relationships and
	// run a validation to make sure it only contains things that it should.
	// To that we add all the scalars and other definitions we always require.
	//
	// Then, we GraphQL validate to make sure their definitions plus our additions
	// is GraphQL valid.  At this point we know the definitions are GraphQL valid,
	// but we need to check if it makes sense to our layer.
	//
	// The next final validation ensures that the definitions are made
	// in such a way that our GraphQL API will be able to interpret the schema
	// correctly.
	//
	// Then we can complete the process by adding in queries and mutations etc. to
	// make the final full GraphQL schema.

	doc, gqlErr := parser.ParseSchemas(validator.Prelude, &ast.Source{Input: input})
	if gqlErr != nil {
		return nil, gqlerror.List{gqlErr}
	}

	// Convert All the Type Extensions into the Type Definitions with @external directive
	// to maintain uniformity in the output schema.
	// No need to add `@extends` directive to type `Query` and `Mutation` since
	// `extend type Query` is same as declaring `type Query`.
	for _, ext := range doc.Extensions {
		if ext.Name != "Query" && ext.Name != "Mutation" {
			ext.Directives = append(ext.Directives, &ast.Directive{Name: "extends"})
		}
	}
	doc.Definitions = append(doc.Definitions, doc.Extensions...)
	doc.Extensions = nil

	gqlErrList := preGQLValidation(doc)
	if gqlErrList != nil {
		return nil, gqlErrList
	}

	typesToComplete := make([]string, 0, len(doc.Definitions))
	defns := make([]string, 0, len(doc.Definitions))
	providesFieldsMap := make(map[string]map[string]bool)
	for _, defn := range doc.Definitions {
		if defn.BuiltIn {
			continue
		}
		defns = append(defns, defn.Name)
		if defn.Kind == ast.Object || defn.Kind == ast.Interface || defn.Kind == ast.Union {
			remoteDir := defn.Directives.ForName(remoteDirective)
			if remoteDir != nil {
				continue
			}
		}
		typesToComplete = append(typesToComplete, defn.Name)
	}

	if gqlErr = expandSchema(doc); gqlErr != nil {
		return nil, gqlerror.List{gqlErr}
	}

	sch, gqlErr := validator.ValidateSchemaDocument(doc)
	if gqlErr != nil {
		return nil, gqlerror.List{gqlErr}
	}

	gqlErrList = postGQLValidation(sch, defns)
	if gqlErrList != nil {
		return nil, gqlErrList
	}

	dgSchema := genDgSchema(sch, typesToComplete, providesFieldsMap)
	completeSchema(sch, typesToComplete, providesFieldsMap)
	cleanSchema(sch)

	if len(sch.Query.Fields) == 0 && len(sch.Mutation.Fields) == 0 {
		return nil, gqlerror.Errorf("No query or mutation found in the generated schema")
	}

	return &Handler{
		input:          input,
		dgraphSchema:   dgSchema,
		completeSchema: sch,
		originalDefs:   defns,
	}, nil
}

func getAllowedHeaders(sch *ast.Schema, definitions []string, authHeader string) []string {
	headers := make(map[string]struct{})

	setHeaders := func(dir *ast.Directive) {
		if dir == nil {
			return
		}

		httpArg := dir.Arguments.ForName("http")
		if httpArg == nil {
			return
		}
		forwardHeaders := httpArg.Value.Children.ForName("forwardHeaders")
		if forwardHeaders == nil {
			return
		}
		for _, h := range forwardHeaders.Children {
			key := strings.Split(h.Value.Raw, ":")
			if len(key) == 1 {
				key = []string{h.Value.Raw, h.Value.Raw}
			}
			headers[key[1]] = struct{}{}
		}
	}

	for _, defn := range definitions {
		for _, field := range sch.Types[defn].Fields {
			custom := field.Directives.ForName(customDirective)
			setHeaders(custom)
		}
	}

	finalHeaders := make([]string, 0, len(headers)+1)
	for h := range headers {
		finalHeaders = append(finalHeaders, h)
	}

	// Add Auth Header to finalHeaders list
	if authHeader != "" {
		finalHeaders = append(finalHeaders, authHeader)
	}

	return finalHeaders
}

func getAllSearchIndexes(val *ast.Value) []string {
	res := make([]string, len(val.Children))

	for i, child := range val.Children {
		res[i] = supportedSearches[child.Value.Raw].dgIndex
	}

	return res
}

func typeName(def *ast.Definition) string {
	name := def.Name
	dir := def.Directives.ForName(dgraphDirective)
	if dir == nil {
		return name
	}
	typeArg := dir.Arguments.ForName(dgraphTypeArg)
	if typeArg == nil {
		return name
	}
	return typeArg.Value.Raw
}

// fieldName returns the dgraph predicate corresponding to a field.
// If the field had a dgraph directive, then it returns the value of the pred arg otherwise
// it returns typeName + "." + fieldName.
func fieldName(def *ast.FieldDefinition, typName string) string {
	predArg := getDgraphDirPredArg(def)
	if predArg == nil {
		return typName + "." + def.Name
	}
	return predArg.Value.Raw
}

func getDgraphDirPredArg(def *ast.FieldDefinition) *ast.Argument {
	dir := def.Directives.ForName(dgraphDirective)
	if dir == nil {
		return nil
	}
	predArg := dir.Arguments.ForName(dgraphPredArg)
	return predArg
}

// genDgSchema generates Dgraph schema from a valid graphql schema.
//
// TODO: This function can just directly spit out the pb.SchemaUpdates list
// instead of creating the schema in string.
func genDgSchema(gqlSch *ast.Schema, definitions []string,
	providesFieldsMap map[string]map[string]bool) string {
	var typeStrings []string

	type dgPred struct {
		typ     string
		indexes map[string]bool
		upsert  string
		reverse string
		lang    bool
	}

	type field struct {
		name string
		// true if the field was inherited from an interface, we don't add the predicate schema
		// for it then as the it would already have been added with the interface.
		inherited bool
	}

	type dgType struct {
		name   string
		fields []field
	}

	dgTypes := make([]dgType, 0, len(definitions))
	dgPreds := make(map[string]dgPred)

	getUpdatedPred := func(fname, typStr, upsertStr string, indexes []string, lang bool) dgPred {
		pred, ok := dgPreds[fname]
		if !ok {
			pred = dgPred{
				typ:     typStr,
				indexes: make(map[string]bool),
				upsert:  upsertStr,
			}
		}
		for _, index := range indexes {
			pred.indexes[index] = true
		}
		pred.lang = lang
		return pred
	}

	for _, key := range definitions {
		if isQueryOrMutation(key) {
			continue
		}
		def := gqlSch.Types[key]
		switch def.Kind {
		case ast.Object, ast.Interface:
			typName := typeName(def)

			typ := dgType{name: typName}
			pwdField := getPasswordField(def)

			for _, f := range def.Fields {
				if hasCustomOrLambda(f) {
					continue
				}

				// Ignore @external fields which are not @key
				if externalAndNonKeyField(f, def, providesFieldsMap[def.Name]) {
					continue
				}

				// If a field of type ID has @external directive and is a @key field then
				// it should be translated into a dgraph field with string type having hash index.
				if f.Type.Name() == "ID" && !(hasExternal(f) && isKeyField(f, def)) {
					continue
				}

				typName = typeName(def)
				// This field could have originally been defined in an interface that this type
				// implements. If we get a parent interface, then we should prefix the field name
				// with it instead of def.Name.
				parentInt := parentInterface(gqlSch, def, f.Name)
				if parentInt != nil {
					typName = typeName(parentInt)
				}
				fname := fieldName(f, typName)

				var prefix, suffix string
				if f.Type.Elem != nil {
					prefix = "["
					suffix = "]"
				}

				var typStr string
				switch gqlSch.Types[f.Type.Name()].Kind {
				case ast.Object, ast.Interface, ast.Union:
					if isGeoType(f.Type) {
						typStr = inbuiltTypeToDgraph[f.Type.Name()]
						var indexes []string
						if f.Directives.ForName(searchDirective) != nil {
							indexes = append(indexes, supportedSearches[defaultSearches[f.Type.
								Name()]].dgIndex)
						}
						dgPreds[fname] = getUpdatedPred(fname, typStr, "", indexes, false)
					} else {
						typStr = fmt.Sprintf("%suid%s", prefix, suffix)
					}

					if parentInt == nil {
						if strings.HasPrefix(fname, "~") {
							// remove ~
							forwardEdge := fname[1:]
							forwardPred := dgPreds[forwardEdge]
							forwardPred.reverse = "@reverse "
							dgPreds[forwardEdge] = forwardPred
						} else {
							pred := dgPreds[fname]
							pred.typ = typStr
							dgPreds[fname] = pred
						}
					}
					typ.fields = append(typ.fields, field{fname, parentInt != nil})
				case ast.Scalar:
					fldType := inbuiltTypeToDgraph[f.Type.Name()]
					// fldType can be "uid" only in case if it is @external and @key
					// in this case it needs to be stored as string in dgraph.
					if fldType == "uid" {
						fldType = "string"
					}
					typStr = fmt.Sprintf(
						"%s%s%s",
						prefix, fldType, suffix,
					)

					var indexes []string
					upsertStr := ""
					search := f.Directives.ForName(searchDirective)
					if search != nil {
						arg := search.Arguments.ForName(searchArgs)
						if arg != nil {
							indexes = append(indexes, getAllSearchIndexes(arg.Value)...)
						} else {
							indexes = append(indexes, supportedSearches[defaultSearches[f.Type.
								Name()]].dgIndex)
						}
					}

					id := f.Directives.ForName(idDirective)
					if id != nil || f.Type.Name() == "ID" {
						upsertStr = "@upsert "
						switch f.Type.Name() {
						case "Int", "Int64":
							indexes = append(indexes, "int")
						case "String", "ID":
							if !x.HasString(indexes, "exact") {
								indexes = append(indexes, "hash")
							}
						}
					}

					if parentInt == nil {
						// if field name contains @ then it is a language tagged field.
						isLang := false
						if strings.Contains(fname, "@") {
							fname = strings.Split(fname, "@")[0]
							isLang = true
						}
						dgPreds[fname] = getUpdatedPred(fname, typStr, upsertStr, indexes, isLang)
					}
					typ.fields = append(typ.fields, field{fname, parentInt != nil})
				case ast.Enum:
					typStr = fmt.Sprintf("%s%s%s", prefix, "string", suffix)

					indexes := []string{"hash"}
					search := f.Directives.ForName(searchDirective)
					if search != nil {
						arg := search.Arguments.ForName(searchArgs)
						if arg != nil {
							indexes = getAllSearchIndexes(arg.Value)
						}
					}
					if parentInt == nil {
						dgPreds[fname] = getUpdatedPred(fname, typStr, "", indexes, false)
					}
					typ.fields = append(typ.fields, field{fname, parentInt != nil})
				}
			}
			if pwdField != nil {
				parentInt := parentInterfaceForPwdField(gqlSch, def, pwdField.Name)
				if parentInt != nil {
					typName = typeName(parentInt)
				}
				fname := fieldName(pwdField, typName)

				if parentInt == nil {
					dgPreds[fname] = dgPred{typ: "password"}
				}

				typ.fields = append(typ.fields, field{fname, parentInt != nil})
			}
			dgTypes = append(dgTypes, typ)
		}
	}

	predWritten := make(map[string]bool, len(dgPreds))
	for _, typ := range dgTypes {
		// fieldAdded keeps track of whether a field has been added to typeDef
		fieldAdded := make(map[string]bool, len(typ.fields))
		var preds strings.Builder
		for _, fld := range typ.fields {
			f, ok := dgPreds[fld.name]
			if !ok || fieldAdded[fld.name] {
				continue
			}
			fieldAdded[fld.name] = true
			if !fld.inherited && !predWritten[fld.name] {
				indexStr := ""
				langStr := ""
				if len(f.indexes) > 0 {
					indexes := make([]string, 0)
					for index := range f.indexes {
						indexes = append(indexes, index)
					}
					sort.Strings(indexes)
					indexStr = fmt.Sprintf(" @index(%s)", strings.Join(indexes, ", "))
				}
				if f.lang {
					langStr = " @lang"
				}
				fmt.Fprintf(&preds, "%s: %s%s%s %s%s.\n", fld.name, f.typ, indexStr, langStr, f.upsert,
					f.reverse)
				predWritten[fld.name] = true
			}
		}
		typeStrings = append(typeStrings, preds.String())
	}

	return strings.Join(typeStrings, "")
}
