/*
 * Copyright 2015-2018 Dgraph Labs, Inc. and Contributors
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

package gql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// This file contains the tests related to alias, fragments, IRIRef, Lang, Order and Regex.
func TestParse_alias_count(t *testing.T) {
	query := `
		{
			me(func: uid(0x0a)) {
				name,
				bestFriend: friends(first: 10) {
					nameCount: count(name)
				}
			}
		}
	`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query[0])
	require.Equal(t, childAttrs(res.Query[0]), []string{"name", "friends"})
	require.Equal(t, res.Query[0].Children[1].Alias, "bestFriend")
	require.Equal(t, childAttrs(res.Query[0].Children[1]), []string{"name"})
	require.Equal(t, "nameCount", res.Query[0].Children[1].Children[0].Alias)
}

func TestParse_alias_var(t *testing.T) {
	query := `
		{
			me(func: uid(0x0a)) {
				name,
				f as bestFriend: friends(first: 10) {
					c as count(friend)
				}
			}

			friend(func: uid(f)) {
				name
				fcount: val(c)
			}
		}
	`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query[0])
	require.Equal(t, childAttrs(res.Query[0]), []string{"name", "friends"})
	require.Equal(t, res.Query[0].Children[1].Alias, "bestFriend")
	require.Equal(t, "fcount", res.Query[1].Children[1].Alias)
}

func TestParse_alias_max(t *testing.T) {
	query := `
		{
			me(func: uid(0x0a)) {
				name,
				bestFriend: friends(first: 10) {
					x as count(friends)
				}
				maxfriendcount: max(val(x))
			}
		}
	`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query[0])
	require.Equal(t, res.Query[0].Children[1].Alias, "bestFriend")
	require.Equal(t, "maxfriendcount", res.Query[0].Children[2].Alias)
}

func TestParse_alias(t *testing.T) {
	query := `
		{
			me(func: uid(0x0a)) {
				name,
				bestFriend: friends(first: 10) {
					name
				}
			}
		}
	`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query[0])
	require.Equal(t, childAttrs(res.Query[0]), []string{"name", "friends"})
	require.Equal(t, res.Query[0].Children[1].Alias, "bestFriend")
	require.Equal(t, childAttrs(res.Query[0].Children[1]), []string{"name"})
}

func TestParse_alias1(t *testing.T) {
	query := `
		{
			me(func: uid(0x0a)) {
				name: type.object.name.en
				bestFriend: friends(first: 10) {
					name: type.object.name.hi
				}
			}
		}
	`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query[0])
	require.Equal(t, childAttrs(res.Query[0]), []string{"type.object.name.en", "friends"})
	require.Equal(t, res.Query[0].Children[1].Alias, "bestFriend")
	require.Equal(t, res.Query[0].Children[1].Children[0].Alias, "name")
	require.Equal(t, childAttrs(res.Query[0].Children[1]), []string{"type.object.name.hi"})
}

func TestParseFragmentMultiQuery(t *testing.T) {
	query := `
	{
		user(func: uid(0x0a)) {
			...fragmenta,...fragmentb
			friends {
				name
			}
			...fragmentc
			hobbies
			...fragmentd
		}

		me(func: uid(0x01)) {
			...fragmenta
			...fragmentb
		}
	}

	fragment fragmenta {
		name
	}

	fragment fragmentb {
		id
	}

	fragment fragmentc {
		name
	}

	fragment fragmentd {
		id
	}
`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query[0])
	require.Equal(t, []string{"name", "id", "friends", "name", "hobbies", "id"}, childAttrs(res.Query[0]))
	require.Equal(t, []string{"name", "id"}, childAttrs(res.Query[1]))
}

func TestParseFragmentNoNesting(t *testing.T) {
	query := `
	query {
		user(func: uid(0x0a)) {
			...fragmenta,...fragmentb
			friends {
				name
			}
			...fragmentc
			hobbies
			...fragmentd
		}
	}

	fragment fragmenta {
		name
	}

	fragment fragmentb {
		id
	}

	fragment fragmentc {
		name
	}

	fragment fragmentd {
		id
	}
`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query[0])
	require.Equal(t, childAttrs(res.Query[0]), []string{"name", "id", "friends", "name", "hobbies", "id"})
}

func TestParseFragmentNest1(t *testing.T) {
	query := `
	query {
		user(func: uid(0x0a)) {
			...fragmenta
			friends {
				name
			}
		}
	}

	fragment fragmenta {
		id
		...fragmentb
	}

	fragment fragmentb {
		hobbies
	}
`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query[0])
	require.Equal(t, childAttrs(res.Query[0]), []string{"id", "hobbies", "friends"})
}

func TestParseFragmentNest2(t *testing.T) {
	query := `
	query {
		user(func: uid(0x0a)) {
			friends {
				...fragmenta
			}
		}
	}
	fragment fragmenta {
		name
		...fragmentb
	}
	fragment fragmentb {
		nickname
	}
`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query[0])
	require.Equal(t, childAttrs(res.Query[0]), []string{"friends"})
	require.Equal(t, childAttrs(res.Query[0].Children[0]), []string{"name", "nickname"})
}

func TestParseFragmentCycle(t *testing.T) {
	query := `
	query {
		user(func: uid(0x0a)) {
			...fragmenta
		}
	}
	fragment fragmenta {
		name
		...fragmentb
	}
	fragment fragmentb {
		...fragmentc
	}
	fragment fragmentc {
		id
		...fragmenta
	}
`
	_, err := Parse(Request{Str: query})
	require.Error(t, err, "Expected error with cycle")
	require.Contains(t, err.Error(), "Cycle detected")
}

func TestParseFragmentMissing(t *testing.T) {
	query := `
	query {
		user(func: uid(0x0a)) {
			...fragmenta
		}
	}
	fragment fragmentb {
		...fragmentc
	}
	fragment fragmentc {
		id
		...fragmenta
	}
`
	_, err := Parse(Request{Str: query})
	require.Error(t, err, "Expected error with missing fragment")
	require.Contains(t, err.Error(), "Missing fragment: fragmenta")
}

func TestParseIRIRef(t *testing.T) {
	query := `{
		me(func: uid( 0x1)) {
			<http://verygood.com/what/about/you>
			friends @filter(allofterms(<http://verygood.com/what/about/you>,
				"good better bad")){
				name
			}
			gender,age
			hometown
		}
	}`

	gq, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.Equal(t, 5, len(gq.Query[0].Children))
	require.Equal(t, "http://verygood.com/what/about/you", gq.Query[0].Children[0].Attr)
	require.Equal(t, `(allofterms http://verygood.com/what/about/you "good better bad")`,
		gq.Query[0].Children[1].Filter.debugString())
}

func TestParseIRIRef2(t *testing.T) {
	query := `{
		me(func:anyofterms(<http://helloworld.com/how/are/you>, "good better bad")) {
			<http://verygood.com/what/about/you>
			friends @filter(allofterms(<http://verygood.com/what/about/you>,
				"good better bad")){
				name
			}
		}
	}`

	gq, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.Equal(t, 2, len(gq.Query[0].Children))
	require.Equal(t, "http://verygood.com/what/about/you", gq.Query[0].Children[0].Attr)
	require.Equal(t, `(allofterms http://verygood.com/what/about/you "good better bad")`,
		gq.Query[0].Children[1].Filter.debugString())
	require.Equal(t, "http://helloworld.com/how/are/you", gq.Query[0].Func.Attr)
}

func TestParseIRIRefSpace(t *testing.T) {
	query := `{
		me(func: uid( <http://helloworld.com/how/are/ you>)) {
		}
	      }`

	_, err := Parse(Request{Str: query})
	require.Error(t, err) // because of space.
	require.Contains(t, err.Error(), "Unexpected character ' ' while parsing IRI")
}

func TestParseIRIRefInvalidChar(t *testing.T) {
	query := `{
		me(func: uid( <http://helloworld.com/how/are/^you>)) {
		}
	      }`

	_, err := Parse(Request{Str: query})
	require.Error(t, err) // because of ^
	require.Contains(t, err.Error(), "Unexpected character '^' while parsing IRI")
}

func TestParseRegexp1(t *testing.T) {
	query := `
	{
	  me(func: uid(0x1)) {
	    name
		friend @filter(regexp(name, /case INSENSITIVE regexp with \/ escaped value/i)) {
	      name
	    }
	  }
    }
`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query)
	require.Equal(t, 1, len(res.Query))
	require.Equal(t, "case INSENSITIVE regexp with / escaped value",
		res.Query[0].Children[1].Filter.Func.Args[0].Value)
	require.Equal(t, "i", res.Query[0].Children[1].Filter.Func.Args[1].Value)
}

func TestParseRegexp2(t *testing.T) {
	query := `
	{
	  me(func:regexp(name, /another\/compilicated ("") regexp('')/)) {
	    name
	  }
    }
`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query)
	require.Equal(t, 1, len(res.Query))
	require.Equal(t, "another/compilicated (\"\") regexp('')",
		res.Query[0].Func.Args[0].Value)
	require.Equal(t, "", res.Query[0].Func.Args[1].Value)
}

func TestParseRegexp3(t *testing.T) {
	query := `
	{
	  me(func:allofterms(name, "barack")) @filter(regexp(secret, /whitehouse[0-9]{1,4}/fLaGs)) {
	    name
	  }
    }
`
	res, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.NotNil(t, res.Query)
	require.Equal(t, 1, len(res.Query))
	require.Equal(t, "whitehouse[0-9]{1,4}", res.Query[0].Filter.Func.Args[0].Value)
	require.Equal(t, "fLaGs", res.Query[0].Filter.Func.Args[1].Value)
}

func TestParseRegexp4(t *testing.T) {
	query := `
	{
	  me(func:regexp(name, /pattern/123)) {
	    name
	  }
    }
`
	_, err := Parse(Request{Str: query})
	// only [a-zA-Z] characters can be used as flags
	require.Error(t, err)
	require.Contains(t, err.Error(), "Expected comma but got: 123")
}

func TestParseRegexp5(t *testing.T) {
	query := `
	{
	  me(func:regexp(name, /pattern/flag123)) {
	    name
	  }
    }
`
	_, err := Parse(Request{Str: query})
	// only [a-zA-Z] characters can be used as flags
	require.Error(t, err)
	require.Contains(t, err.Error(), "Expected comma but got: 123")
}

func TestParseRegexp6(t *testing.T) {
	query := `
	{
	  me(func:regexp(name, /pattern\/)) {
	    name
	  }
    }
`
	_, err := Parse(Request{Str: query})
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"Unclosed regexp")
}
func TestOrder1(t *testing.T) {
	query := `
		{
			me(func: uid(1), orderdesc: name, orderasc: age) {
				name
			}
		}
	`
	gq, err := Parse(Request{Str: query})
	require.NoError(t, err)
	require.Equal(t, 2, len(gq.Query[0].Order))
	require.Equal(t, "name", gq.Query[0].Order[0].Attr)
	require.Equal(t, true, gq.Query[0].Order[0].Desc)
	require.Equal(t, "age", gq.Query[0].Order[1].Attr)
	require.Equal(t, false, gq.Query[0].Order[1].Desc)
}

func TestOrder2(t *testing.T) {
	query := `
		{
			me(func: uid(0x01)) {
				friend(orderasc: alias, orderdesc: name) @filter(lt(alias, "Pat")) {
					alias
				}
			}
		}
	`
	gq, err := Parse(Request{Str: query})
	require.NoError(t, err)
	curp := gq.Query[0].Children[0]
	require.Equal(t, 2, len(curp.Order))
	require.Equal(t, "alias", curp.Order[0].Attr)
	require.Equal(t, false, curp.Order[0].Desc)
	require.Equal(t, "name", curp.Order[1].Attr)
	require.Equal(t, true, curp.Order[1].Desc)
}

func TestMultipleOrderError(t *testing.T) {
	query := `
		{
			me(func: uid(0x01)) {
				friend(orderasc: alias, orderdesc: alias) {
					alias
				}
			}
		}
	`
	_, err := Parse(Request{Str: query})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Sorting by an attribute: [alias] can only be done once")
}

func TestMultipleOrderError2(t *testing.T) {
	query := `
		{
			me(func: uid(0x01),orderasc: alias, orderdesc: alias) {
				friend {
					alias
				}
			}
		}
	`
	_, err := Parse(Request{Str: query})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Sorting by an attribute: [alias] can only be done once")
}

func TestOrderWithMultipleLangFail(t *testing.T) {
	query := `
	{
		me(func: uid(0x1), orderasc: name@en:fr, orderdesc: lastname@ci, orderasc: salary) {
			name
		}
	}
	`
	_, err := Parse(Request{Str: query})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Sorting by an attribute: [name@en:fr] can only be done on one language")
}
