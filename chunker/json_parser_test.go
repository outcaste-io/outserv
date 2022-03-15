/*
 * Copyright 2017-2018 Dgraph Labs, Inc. and Contributors
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

package chunker

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/outcaste-io/dgo/v210/protos/api"
	"github.com/outcaste-io/outserv/protos/pb"

	"github.com/stretchr/testify/require"
)

func makeNquad(sub, pred string, val *pb.Value) *pb.NQuad {
	return &pb.NQuad{
		Subject:     sub,
		Predicate:   pred,
		ObjectValue: val,
	}
}

func makeNquadEdge(sub, pred, obj string) *pb.NQuad {
	return &pb.NQuad{
		Subject:   sub,
		Predicate: pred,
		ObjectId:  obj,
	}
}

type School struct {
	Uid  string `json:"uid,omitempty"`
	Name string `json:"name,omitempty"`
}

type address struct {
	Type   string    `json:"type,omitempty"`
	Coords []float64 `json:"coordinates,omitempty"`
}

type Person struct {
	Uid       string     `json:"uid,omitempty"`
	Namespace string     `json:"namespace,omitempty"`
	Name      string     `json:"name,omitempty"`
	Age       int        `json:"age,omitempty"`
	Married   *bool      `json:"married,omitempty"`
	Now       *time.Time `json:"now,omitempty"`
	Address   address    `json:"address,omitempty"` // geo value
	Friends   []Person   `json:"friend,omitempty"`
	School    *School    `json:"school,omitempty"`
}

func Parse(b []byte, op int) ([]*pb.NQuad, error) {
	nqs := NewNQuadBuffer(1000)
	err := nqs.ParseJSON(b, op)
	return nqs.nquads, err
}

// FastParse uses buf.FastParseJSON() simdjson parser.
func FastParse(b []byte, op int) ([]*pb.NQuad, error) {
	nqs := NewNQuadBuffer(1000)
	err := nqs.FastParseJSON(b, op)
	return nqs.nquads, err
}

func Sort(nq []*pb.NQuad) {
	sort.Slice(nq, func(i, j int) bool {
		return nq[i].Subject < nq[j].Subject
	})
	sort.Slice(nq, func(i, j int) bool {
		return nq[i].Predicate < nq[j].Predicate
	})
}

func TestNquadsFromJson1(t *testing.T) {
	tn := time.Now().UTC()
	m := true
	p := Person{
		Uid:       "1",
		Namespace: "0x2",
		Name:      "Alice",
		Age:       26,
		Married:   &m,
		Now:       &tn,
		Address: address{
			Type:   "Point",
			Coords: []float64{1.1, 2.0},
		},
	}

	b, err := json.Marshal(p)
	require.NoError(t, err)

	nq, err := Parse(b, SetNquads)
	require.NoError(t, err)
	require.Equal(t, 5, len(nq))

	fastNQ, err := FastParse(b, SetNquads)
	require.NoError(t, err)
	require.Equal(t, 5, len(fastNQ))
}

func TestNquadsFromJson2(t *testing.T) {
	m := false

	p := Person{
		Name: "Alice",
		Friends: []Person{{
			Name:    "Charlie",
			Married: &m,
		}, {
			Uid:  "1000",
			Name: "Bob",
		}},
	}

	b, err := json.Marshal(p)
	require.NoError(t, err)

	nq, err := Parse(b, SetNquads)
	require.NoError(t, err)
	require.Equal(t, 6, len(nq))

	fastNQ, err := FastParse(b, SetNquads)
	require.NoError(t, err)
	require.Equal(t, 6, len(fastNQ))
}

func TestNquadsFromJson3(t *testing.T) {
	p := Person{
		Uid:  "_:alice",
		Name: "Alice",
		School: &School{
			Uid:  "_:school",
			Name: "Wellington Public School",
		},
	}

	expectedNquads := []pb.NQuad{
		{
			Subject:     "_:alice",
			Predicate:   "name",
			ObjectValue: &pb.Value{Val: &pb.Value_StrVal{StrVal: "Alice"}},
		},
		{
			Subject:     "_:school",
			Predicate:   "name",
			ObjectValue: &pb.Value{Val: &pb.Value_StrVal{StrVal: "Wellington Public School"}},
		},
		{
			Subject:   "_:alice",
			Predicate: "school",
			ObjectId:  "_:school",
		},
	}

	b, err := json.Marshal(p)
	require.NoError(t, err)

	nq, err := Parse(b, SetNquads)
	require.NoError(t, err)
	Sort(nq)
	fastNQ, err := FastParse(b, SetNquads)
	require.NoError(t, err)
	Sort(fastNQ)

	for i, test := range expectedNquads {
		require.Equal(t, test.Subject, nq[i].Subject)
		require.Equal(t, test.Predicate, nq[i].Predicate)
		require.Equal(t, test.ObjectValue, nq[i].ObjectValue)
		require.Equal(t, test.ObjectId, nq[i].ObjectId)

		require.Equal(t, test.Subject, fastNQ[i].Subject)
		require.Equal(t, test.Predicate, fastNQ[i].Predicate)
		require.Equal(t, test.ObjectValue, fastNQ[i].ObjectValue)
		require.Equal(t, test.ObjectId, fastNQ[i].ObjectId)
	}
}

func TestNquadsFromJson4(t *testing.T) {
	json := `[{"name":"Alice","mobile":"040123456","car":"MA0123", "age": 21, "weight": 58.7}]`

	nq, err := Parse([]byte(json), SetNquads)
	require.NoError(t, err)

	fastNQ, err := FastParse([]byte(json), SetNquads)
	require.NoError(t, err)
	_, _ = nq, fastNQ
}

func TestNquadsFromJsonMap(t *testing.T) {
	json := `{"name":"Alice",
"age": 25,
"friends": [{
"name": "Bob"
}]}`

	nq, err := Parse([]byte(json), SetNquads)
	require.NoError(t, err)

	fastNQ, err := FastParse([]byte(json), SetNquads)
	require.NoError(t, err)
	_, _ = nq, fastNQ
}

func TestNquadsFromMultipleJsonObjects(t *testing.T) {
	json := `
[
  {
    "name": "A",
    "age": 25,
    "friends": [
      {
        "name": "A1",
        "friends": [
          {
            "name": "A11"
          },
          {
            "name": "A12"
          }
        ]
      },
     {
        "name": "A2",
        "friends": [
          {
            "name": "A21"
          },
          {
            "name": "A22"
          }
        ]
      }
    ]
  },
  {
    "name": "B",
    "age": 26,
    "friends": [
      {
        "name": "B1",
        "friends": [
          {
            "name": "B11"
          },
          {
            "name": "B12"
          }
        ]
      },
     {
        "name": "B2",
        "friends": [
          {
            "name": "B21"
          },
          {
            "name": "B22"
          }
        ]
      }
    ]
  }
]
`

	nq, err := Parse([]byte(json), SetNquads)
	require.NoError(t, err)

	fastNQ, err := FastParse([]byte(json), SetNquads)
	require.NoError(t, err)
	_, _ = nq, fastNQ
}

func TestJsonNumberParsing(t *testing.T) {
	tests := []struct {
		in  string
		out *pb.Value
	}{
		{`{"uid": "1", "key": 9223372036854775299}`, &pb.Value{Val: &pb.Value_IntVal{IntVal: 9223372036854775299}}},
		{`{"uid": "1", "key": 9223372036854775299.0}`, &pb.Value{Val: &pb.Value_DoubleVal{DoubleVal: 9223372036854775299.0}}},
		{`{"uid": "1", "key": 27670116110564327426}`, nil},
		{`{"uid": "1", "key": "23452786"}`, &pb.Value{Val: &pb.Value_StrVal{StrVal: "23452786"}}},
		{`{"uid": "1", "key": "23452786.2378"}`, &pb.Value{Val: &pb.Value_StrVal{StrVal: "23452786.2378"}}},
		{`{"uid": "1", "key": -1e10}`, &pb.Value{Val: &pb.Value_DoubleVal{DoubleVal: -1e+10}}},
		{`{"uid": "1", "key": 0E-0}`, &pb.Value{Val: &pb.Value_DoubleVal{DoubleVal: 0}}},
	}

	for i, test := range tests {
		nqs, err := Parse([]byte(test.in), SetNquads)
		if i == 2 {
			fmt.Println(err)
		}
		if test.out != nil {
			require.NoError(t, err, "%T", err)
			require.Equal(t, makeNquad("1", "key", test.out), nqs[0])
		} else {
			require.Error(t, err)
		}

		fastNQ, err := FastParse([]byte(test.in), SetNquads)
		if i == 2 {
			fmt.Println(err)
		}
		if test.out != nil {
			require.NoError(t, err, "%T", err)
			require.Equal(t, makeNquad("1", "key", test.out), fastNQ[0])
		} else {
			require.Error(t, err)
		}
	}
}

func TestNquadsFromJson_UidOutofRangeError(t *testing.T) {
	json := `{"uid":"0xa14222b693e4ba34123","name":"Name","following":[{"name":"Bob"}],"school":[{"uid":"","name@en":"Crown Public School"}]}`

	_, err := Parse([]byte(json), SetNquads)
	require.Error(t, err)

	_, err = FastParse([]byte(json), SetNquads)
	require.Error(t, err)
}

func TestNquadsFromJsonArray(t *testing.T) {
	json := `[
		{
			"uid": "uid(Project10)",
			"Ticket.row": {
				"uid": "uid(x)"
			}
		},
		{
			"Project.columns": [
				{
					"uid": "uid(x)"
				}
			],
			"uid": "uid(Project3)"
		},
		{
			"Ticket.onColumn": {
				"uid": "uid(x)"
			},
			"uid": "uid(Ticket4)"
		}
	]`

	nqs, err := Parse([]byte(json), SetNquads)
	require.NoError(t, err)
	require.Equal(t, 3, len(nqs))

	nqs, err = FastParse([]byte(json), SetNquads)
	require.NoError(t, err)
	require.Equal(t, 3, len(nqs))
}

func TestNquadsFromJson_NegativeUidError(t *testing.T) {
	json := `{"uid":"-100","name":"Name","following":[{"name":"Bob"}],"school":[{"uid":"","name@en":"Crown Public School"}]}`

	_, err := Parse([]byte(json), SetNquads)
	require.Error(t, err)

	_, err = FastParse([]byte(json), SetNquads)
	require.Error(t, err)
}

func TestNquadsFromJson_EmptyUid(t *testing.T) {
	json := `{"uid":"","name":"Alice","following":[{"name":"Bob"}],"school":[{"uid":"",
"name":"Crown Public School"}]}`
	nq, err := Parse([]byte(json), SetNquads)
	require.NoError(t, err)

	fastNQ, err := FastParse([]byte(json), SetNquads)
	require.NoError(t, err)
	_, _ = nq, fastNQ
}

func TestNquadsFromJson_BlankNodes(t *testing.T) {
	json := `{"uid":"_:alice","name":"Alice","following":[{"name":"Bob"}],"school":[{"uid":"_:school","name":"Crown Public School"}]}`

	nq, err := Parse([]byte(json), SetNquads)
	require.NoError(t, err)

	fastNQ, err := FastParse([]byte(json), SetNquads)
	require.NoError(t, err)
	_, _ = nq, fastNQ
}

func TestNquadsDeleteEdges(t *testing.T) {
	json := `[{"uid": "0x1","name":null,"mobile":null,"car":null}]`
	nq, err := Parse([]byte(json), DeleteNquads)
	require.NoError(t, err)
	require.Equal(t, 3, len(nq))

	fastNQ, err := FastParse([]byte(json), DeleteNquads)
	require.NoError(t, err)
	require.Equal(t, 3, len(fastNQ))
}

func getMapOfFacets(facets []*api.Facet) map[string]*api.Facet {
	res := make(map[string]*api.Facet)
	for _, f := range facets {
		res[f.Key] = f
	}
	return res
}

func TestNquadsFromJsonError1(t *testing.T) {
	p := Person{
		Name: "Alice",
		School: &School{
			Name: "Wellington Public School",
		},
	}

	b, err := json.Marshal(p)
	require.NoError(t, err)

	_, err = Parse(b, DeleteNquads)
	require.Error(t, err)
	require.Contains(t, err.Error(), "UID must be present and non-zero while deleting edges.")

	_, err = FastParse(b, DeleteNquads)
	require.Error(t, err)
	require.Contains(t, err.Error(), "UID must be present and non-zero while deleting edges.")
}

func TestNquadsFromJsonList(t *testing.T) {
	json := `{"address":["Riley Street","Redfern"],"phone_number":[123,9876],"points":[{"type":"Point", "coordinates":[1.1,2.0]},{"type":"Point", "coordinates":[2.0,1.1]}]}`

	nq, err := Parse([]byte(json), SetNquads)
	require.NoError(t, err)
	require.Equal(t, 6, len(nq))

	fastNQ, err := FastParse([]byte(json), SetNquads)
	require.NoError(t, err)
	require.Equal(t, 6, len(fastNQ))
}

func TestNquadsFromJsonDelete(t *testing.T) {
	json := `{"uid":1000,"friend":[{"uid":1001}]}`

	nq, err := Parse([]byte(json), DeleteNquads)
	require.NoError(t, err)
	require.Equal(t, nq[0], makeNquadEdge("1000", "friend", "1001"))

	fastNQ, err := FastParse([]byte(json), DeleteNquads)
	require.NoError(t, err)
	require.Equal(t, fastNQ[0], makeNquadEdge("1000", "friend", "1001"))
}

func TestNquadsFromJsonDeleteStar(t *testing.T) {
	json := `{"uid":1000,"name": null}`

	nq, err := Parse([]byte(json), DeleteNquads)
	require.NoError(t, err)

	fastNQ, err := FastParse([]byte(json), DeleteNquads)
	require.NoError(t, err)

	expected := &api.NQuad{
		Subject:   "1000",
		Predicate: "name",
		ObjectValue: &api.Value{
			Val: &api.Value_DefaultVal{
				DefaultVal: "_STAR_ALL",
			},
		},
	}

	require.Equal(t, expected, nq[0])
	require.Equal(t, expected, fastNQ[0])
}

func TestValInUpsert(t *testing.T) {
	json := `{"uid":1000, "name": "val(name)"}`
	nq, err := Parse([]byte(json), SetNquads)
	require.NoError(t, err)

	fastNQ, err := FastParse([]byte(json), SetNquads)
	require.NoError(t, err)

	expected := &api.NQuad{
		Subject:   "1000",
		Predicate: "name",
		ObjectId:  "val(name)",
	}

	require.Equal(t, expected, nq[0])
	require.Equal(t, expected, fastNQ[0])
}

func TestNquadsFromJsonDeleteStarLang(t *testing.T) {
	json := `{"uid":1000,"name@es": null}`

	nq, err := Parse([]byte(json), DeleteNquads)
	require.NoError(t, err)

	fastNQ, err := FastParse([]byte(json), DeleteNquads)
	require.NoError(t, err)

	expected := &api.NQuad{
		Subject:   "1000",
		Predicate: "name",
		ObjectValue: &api.Value{
			Val: &api.Value_DefaultVal{
				DefaultVal: "_STAR_ALL",
			},
		},
		Lang: "es",
	}

	require.Equal(t, expected, nq[0])
	require.Equal(t, expected, fastNQ[0])
}

func TestSetNquadNilValue(t *testing.T) {
	json := `{"uid":1000,"name": null}`

	nq, err := Parse([]byte(json), SetNquads)
	require.NoError(t, err)
	require.Equal(t, 0, len(nq))

	fastNQ, err := FastParse([]byte(json), SetNquads)
	require.NoError(t, err)
	require.Equal(t, 0, len(fastNQ))
}

// See PR #7737 to understand why this test exists.
func TestNquadsFromJsonEmptyFacet(t *testing.T) {
	json := `{"uid":1000,"doesnt|exist":null}`

	// fast
	buf := NewNQuadBuffer(-1)
	require.Nil(t, buf.FastParseJSON([]byte(json), DeleteNquads))
	buf.Flush()
	// needs to be empty, otherwise node gets deleted
	require.Equal(t, 0, len(<-buf.Ch()))

	// old
	buf = NewNQuadBuffer(-1)
	require.Nil(t, buf.ParseJSON([]byte(json), DeleteNquads))
	buf.Flush()
	// needs to be empty, otherwise node gets deleted
	require.Equal(t, 0, len(<-buf.Ch()))
}

func BenchmarkNoFacets(b *testing.B) {
	json := []byte(`[
	{
		"uid":123,
		"flguid":123,
		"is_validate":"xxxxxxxxxx",
		"createDatetime":"xxxxxxxxxx",
		"contains":{
			"createDatetime":"xxxxxxxxxx",
			"final_individ":"xxxxxxxxxx",
			"cm_bad_debt":"xxxxxxxxxx",
			"cm_bill_address1":"xxxxxxxxxx",
			"cm_bill_address2":"xxxxxxxxxx",
			"cm_bill_city":"xxxxxxxxxx",
			"cm_bill_state":"xxxxxxxxxx",
			"cm_zip":"xxxxxxxxxx",
			"zip5":"xxxxxxxxxx",
			"cm_customer_id":"xxxxxxxxxx",
			"final_gaid":"xxxxxxxxxx",
			"final_hholdid":"xxxxxxxxxx",
			"final_firstname":"xxxxxxxxxx",
			"final_middlename":"xxxxxxxxxx",
			"final_surname":"xxxxxxxxxx",
			"final_gender":"xxxxxxxxxx",
			"final_ace_prim_addr":"xxxxxxxxxx",
			"final_ace_sec_addr":"xxxxxxxxxx",
			"final_ace_urb":"xxxxxxxxxx",
			"final_ace_city_llidx":"xxxxxxxxxx",
			"final_ace_state":"xxxxxxxxxx",
			"final_ace_postal_code":"xxxxxxxxxx",
			"final_ace_zip4":"xxxxxxxxxx",
			"final_ace_dpbc":"xxxxxxxxxx",
			"final_ace_checkdigit":"xxxxxxxxxx",
			"final_ace_iso_code":"xxxxxxxxxx",
			"final_ace_cart":"xxxxxxxxxx",
			"final_ace_lot":"xxxxxxxxxx",
			"final_ace_lot_order":"xxxxxxxxxx",
			"final_ace_rec_type":"xxxxxxxxxx",
			"final_ace_remainder":"xxxxxxxxxx",
			"final_ace_dpv_cmra":"xxxxxxxxxx",
			"final_ace_dpv_ftnote":"xxxxxxxxxx",
			"final_ace_dpv_status":"xxxxxxxxxx",
			"final_ace_foreigncode":"xxxxxxxxxx",
			"final_ace_match_5":"xxxxxxxxxx",
			"final_ace_match_9":"xxxxxxxxxx",
			"final_ace_match_un":"xxxxxxxxxx",
			"final_ace_zip_move":"xxxxxxxxxx",
			"final_ace_ziptype":"xxxxxxxxxx",
			"final_ace_congress":"xxxxxxxxxx",
			"final_ace_county":"xxxxxxxxxx",
			"final_ace_countyname":"xxxxxxxxxx",
			"final_ace_factype":"xxxxxxxxxx",
			"final_ace_fipscode":"xxxxxxxxxx",
			"final_ace_error_code":"xxxxxxxxxx",
			"final_ace_stat_code":"xxxxxxxxxx",
			"final_ace_geo_match":"xxxxxxxxxx",
			"final_ace_geo_lat":"xxxxxxxxxx",
			"final_ace_geo_lng":"xxxxxxxxxx",
			"final_ace_ageo_pla":"xxxxxxxxxx",
			"final_ace_geo_blk":"xxxxxxxxxx",
			"final_ace_ageo_mcd":"xxxxxxxxxx",
			"final_ace_cgeo_cbsa":"xxxxxxxxxx",
			"final_ace_cgeo_msa":"xxxxxxxxxx",
			"final_ace_ap_lacscode":"xxxxxxxxxx",
			"final_dsf_businessflag":"xxxxxxxxxx",
			"final_dsf_dropflag":"xxxxxxxxxx",
			"final_dsf_throwbackflag":"xxxxxxxxxx",
			"final_dsf_seasonalflag":"xxxxxxxxxx",
			"final_dsf_vacantflag":"xxxxxxxxxx",
			"final_dsf_deliverytype":"xxxxxxxxxx",
			"final_dsf_dt_curbflag":"xxxxxxxxxx",
			"final_dsf_dt_ndcbuflag":"xxxxxxxxxx",
			"final_dsf_dt_centralflag":"xxxxxxxxxx",
			"final_dsf_dt_doorslotflag":"xxxxxxxxxx",
			"final_dsf_dropcount":"xxxxxxxxxx",
			"final_dsf_nostatflag":"xxxxxxxxxx",
			"final_dsf_educationalflag":"xxxxxxxxxx",
			"final_dsf_rectyp":"xxxxxxxxxx",
			"final_mailability_score":"xxxxxxxxxx",
			"final_occupancy_score":"xxxxxxxxxx",
			"final_multi_type":"xxxxxxxxxx",
			"final_deceased_flag":"xxxxxxxxxx",
			"final_dnm_flag":"xxxxxxxxxx",
			"final_dnc_flag":"xxxxxxxxxx",
			"final_dnf_flag":"xxxxxxxxxx",
			"final_prison_flag":"xxxxxxxxxx",
			"final_nursing_home_flag":"xxxxxxxxxx",
			"final_date_of_birth":"xxxxxxxxxx",
			"final_date_of_death":"xxxxxxxxxx",
			"vip_number":"xxxxxxxxxx",
			"vip_store_no":"xxxxxxxxxx",
			"vip_division":"xxxxxxxxxx",
			"vip_phone_number":"xxxxxxxxxx",
			"vip_email_address":"xxxxxxxxxx",
			"vip_first_name":"xxxxxxxxxx",
			"vip_last_name":"xxxxxxxxxx",
			"vip_gender":"xxxxxxxxxx",
			"vip_status":"xxxxxxxxxx",
			"vip_membership_date":"xxxxxxxxxx",
			"vip_expiration_date":"xxxxxxxxxx",
			"cm_date_addr_chng":"xxxxxxxxxx",
			"cm_date_entered":"xxxxxxxxxx",
			"cm_name":"xxxxxxxxxx",
			"cm_opt_on_acct":"xxxxxxxxxx",
			"cm_origin":"xxxxxxxxxx",
			"cm_orig_acq_source":"xxxxxxxxxx",
			"cm_phone_number":"xxxxxxxxxx",
			"cm_phone_number2":"xxxxxxxxxx",
			"cm_problem_cust":"xxxxxxxxxx",
			"cm_rm_list":"xxxxxxxxxx",
			"cm_rm_rented_list":"xxxxxxxxxx",
			"cm_tax_code":"xxxxxxxxxx",
			"email_address":"xxxxxxxxxx",
			"esp_email_id":"xxxxxxxxxx",
			"esp_sub_date":"xxxxxxxxxx",
			"esp_unsub_date":"xxxxxxxxxx",
			"cm_user_def_1":"xxxxxxxxxx",
			"cm_user_def_7":"xxxxxxxxxx",
			"do_not_phone":"xxxxxxxxxx",
			"company_num":"xxxxxxxxxx",
			"customer_id":"xxxxxxxxxx",
			"load_date":"xxxxxxxxxx",
			"activity_date":"xxxxxxxxxx",
			"email_address_hashed":"xxxxxxxxxx",
			"event_id":"",
			"contains":{
				"uid": 123,
				"flguid": 123,
				"is_validate":"xxxxxxxxxx",
				"createDatetime":"xxxxxxxxxx"
			}
		}
	}]`)

	// we're parsing 125 nquads at a time, so the MB/s == MNquads/s
	b.SetBytes(125)
	for n := 0; n < b.N; n++ {
		Parse([]byte(json), SetNquads)
	}
}

func BenchmarkNoFacetsFast(b *testing.B) {
	json := []byte(`[
	{
		"uid":123,
		"flguid":123,
		"is_validate":"xxxxxxxxxx",
		"createDatetime":"xxxxxxxxxx",
		"contains":{
			"createDatetime":"xxxxxxxxxx",
			"final_individ":"xxxxxxxxxx",
			"cm_bad_debt":"xxxxxxxxxx",
			"cm_bill_address1":"xxxxxxxxxx",
			"cm_bill_address2":"xxxxxxxxxx",
			"cm_bill_city":"xxxxxxxxxx",
			"cm_bill_state":"xxxxxxxxxx",
			"cm_zip":"xxxxxxxxxx",
			"zip5":"xxxxxxxxxx",
			"cm_customer_id":"xxxxxxxxxx",
			"final_gaid":"xxxxxxxxxx",
			"final_hholdid":"xxxxxxxxxx",
			"final_firstname":"xxxxxxxxxx",
			"final_middlename":"xxxxxxxxxx",
			"final_surname":"xxxxxxxxxx",
			"final_gender":"xxxxxxxxxx",
			"final_ace_prim_addr":"xxxxxxxxxx",
			"final_ace_sec_addr":"xxxxxxxxxx",
			"final_ace_urb":"xxxxxxxxxx",
			"final_ace_city_llidx":"xxxxxxxxxx",
			"final_ace_state":"xxxxxxxxxx",
			"final_ace_postal_code":"xxxxxxxxxx",
			"final_ace_zip4":"xxxxxxxxxx",
			"final_ace_dpbc":"xxxxxxxxxx",
			"final_ace_checkdigit":"xxxxxxxxxx",
			"final_ace_iso_code":"xxxxxxxxxx",
			"final_ace_cart":"xxxxxxxxxx",
			"final_ace_lot":"xxxxxxxxxx",
			"final_ace_lot_order":"xxxxxxxxxx",
			"final_ace_rec_type":"xxxxxxxxxx",
			"final_ace_remainder":"xxxxxxxxxx",
			"final_ace_dpv_cmra":"xxxxxxxxxx",
			"final_ace_dpv_ftnote":"xxxxxxxxxx",
			"final_ace_dpv_status":"xxxxxxxxxx",
			"final_ace_foreigncode":"xxxxxxxxxx",
			"final_ace_match_5":"xxxxxxxxxx",
			"final_ace_match_9":"xxxxxxxxxx",
			"final_ace_match_un":"xxxxxxxxxx",
			"final_ace_zip_move":"xxxxxxxxxx",
			"final_ace_ziptype":"xxxxxxxxxx",
			"final_ace_congress":"xxxxxxxxxx",
			"final_ace_county":"xxxxxxxxxx",
			"final_ace_countyname":"xxxxxxxxxx",
			"final_ace_factype":"xxxxxxxxxx",
			"final_ace_fipscode":"xxxxxxxxxx",
			"final_ace_error_code":"xxxxxxxxxx",
			"final_ace_stat_code":"xxxxxxxxxx",
			"final_ace_geo_match":"xxxxxxxxxx",
			"final_ace_geo_lat":"xxxxxxxxxx",
			"final_ace_geo_lng":"xxxxxxxxxx",
			"final_ace_ageo_pla":"xxxxxxxxxx",
			"final_ace_geo_blk":"xxxxxxxxxx",
			"final_ace_ageo_mcd":"xxxxxxxxxx",
			"final_ace_cgeo_cbsa":"xxxxxxxxxx",
			"final_ace_cgeo_msa":"xxxxxxxxxx",
			"final_ace_ap_lacscode":"xxxxxxxxxx",
			"final_dsf_businessflag":"xxxxxxxxxx",
			"final_dsf_dropflag":"xxxxxxxxxx",
			"final_dsf_throwbackflag":"xxxxxxxxxx",
			"final_dsf_seasonalflag":"xxxxxxxxxx",
			"final_dsf_vacantflag":"xxxxxxxxxx",
			"final_dsf_deliverytype":"xxxxxxxxxx",
			"final_dsf_dt_curbflag":"xxxxxxxxxx",
			"final_dsf_dt_ndcbuflag":"xxxxxxxxxx",
			"final_dsf_dt_centralflag":"xxxxxxxxxx",
			"final_dsf_dt_doorslotflag":"xxxxxxxxxx",
			"final_dsf_dropcount":"xxxxxxxxxx",
			"final_dsf_nostatflag":"xxxxxxxxxx",
			"final_dsf_educationalflag":"xxxxxxxxxx",
			"final_dsf_rectyp":"xxxxxxxxxx",
			"final_mailability_score":"xxxxxxxxxx",
			"final_occupancy_score":"xxxxxxxxxx",
			"final_multi_type":"xxxxxxxxxx",
			"final_deceased_flag":"xxxxxxxxxx",
			"final_dnm_flag":"xxxxxxxxxx",
			"final_dnc_flag":"xxxxxxxxxx",
			"final_dnf_flag":"xxxxxxxxxx",
			"final_prison_flag":"xxxxxxxxxx",
			"final_nursing_home_flag":"xxxxxxxxxx",
			"final_date_of_birth":"xxxxxxxxxx",
			"final_date_of_death":"xxxxxxxxxx",
			"vip_number":"xxxxxxxxxx",
			"vip_store_no":"xxxxxxxxxx",
			"vip_division":"xxxxxxxxxx",
			"vip_phone_number":"xxxxxxxxxx",
			"vip_email_address":"xxxxxxxxxx",
			"vip_first_name":"xxxxxxxxxx",
			"vip_last_name":"xxxxxxxxxx",
			"vip_gender":"xxxxxxxxxx",
			"vip_status":"xxxxxxxxxx",
			"vip_membership_date":"xxxxxxxxxx",
			"vip_expiration_date":"xxxxxxxxxx",
			"cm_date_addr_chng":"xxxxxxxxxx",
			"cm_date_entered":"xxxxxxxxxx",
			"cm_name":"xxxxxxxxxx",
			"cm_opt_on_acct":"xxxxxxxxxx",
			"cm_origin":"xxxxxxxxxx",
			"cm_orig_acq_source":"xxxxxxxxxx",
			"cm_phone_number":"xxxxxxxxxx",
			"cm_phone_number2":"xxxxxxxxxx",
			"cm_problem_cust":"xxxxxxxxxx",
			"cm_rm_list":"xxxxxxxxxx",
			"cm_rm_rented_list":"xxxxxxxxxx",
			"cm_tax_code":"xxxxxxxxxx",
			"email_address":"xxxxxxxxxx",
			"esp_email_id":"xxxxxxxxxx",
			"esp_sub_date":"xxxxxxxxxx",
			"esp_unsub_date":"xxxxxxxxxx",
			"cm_user_def_1":"xxxxxxxxxx",
			"cm_user_def_7":"xxxxxxxxxx",
			"do_not_phone":"xxxxxxxxxx",
			"company_num":"xxxxxxxxxx",
			"customer_id":"xxxxxxxxxx",
			"load_date":"xxxxxxxxxx",
			"activity_date":"xxxxxxxxxx",
			"email_address_hashed":"xxxxxxxxxx",
			"event_id":"",
			"contains":{
				"uid": 123,
				"flguid": 123,
				"is_validate":"xxxxxxxxxx",
				"createDatetime":"xxxxxxxxxx"
			}
		}
	}]`)

	// we're parsing 125 nquads at a time, so the MB/s == MNquads/s
	b.SetBytes(125)
	for n := 0; n < b.N; n++ {
		FastParse([]byte(json), SetNquads)
	}
}
