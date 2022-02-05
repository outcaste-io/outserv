/*
 *    Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package directives

import (
	"encoding/base64"
	"io/ioutil"
	"os"
	"testing"

	"github.com/outcaste-io/outserv/graphql/e2e/common"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestRunAll_WithDgraphDirectives(t *testing.T) {
	common.RunAll(t)
	t.Run("dgraph predicate with special characters",
		common.DgraphDirectiveWithSpecialCharacters)
}

func TestSchema_WithDgraphDirectives(t *testing.T) {
	b, err := ioutil.ReadFile("schema_response.json")
	require.NoError(t, err)

	t.Run("graphql schema", func(t *testing.T) {
		common.SchemaTest(t, string(b))
	})
}

func TestMain(m *testing.M) {
	schemaFile := "schema.graphql"
	schema, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		panic(err)
	}

	jsonFile := "test_data.json"
	data, err := ioutil.ReadFile(jsonFile)
	if err != nil {
		panic(errors.Wrapf(err, "Unable to read file %s.", jsonFile))
	}

	scriptFile := "script.js"
	script, err := ioutil.ReadFile(scriptFile)
	if err != nil {
		panic(errors.Wrapf(err, "Unable to read file %s.", scriptFile))
	}

	// set up the lambda url for unit tests
	x.Config.Lambda = x.LambdaOptions{
		Num:  2,
		Port: 20000,
	}

	common.BootstrapServer(schema, data)
	common.AddLambdaScript(base64.StdEncoding.EncodeToString(script))

	os.Exit(m.Run())
}
