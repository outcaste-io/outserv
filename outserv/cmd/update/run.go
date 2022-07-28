package update

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var Update x.SubCommand

func init() {
	Update.Cmd = &cobra.Command{
		Use:   "update",
		Short: "Update GraphQL Schema and Lambda Script",
		Long: "This tool calls the relevant /admin APIs to easily update" +
			" the GraphQL Schema and Lambda Script.",
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
		Annotations: map[string]string{"group": "core"},
	}
	Update.Cmd.SetHelpTemplate(x.NonRootTemplate)

	flag := Update.Cmd.Flags()
	flag.StringP("addr", "a", "http://localhost:8080/admin",
		"HTTP address of Outserv server's admin endpoint")
	flag.StringP("schema", "s", "", "Path to schema file")
	flag.StringP("lambda", "l", "", "Path to lambda script")
}

var addr string

func updateSchema(schemaPath string) error {
	if len(schemaPath) == 0 {
		return nil
	}
	data, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return errors.Wrapf(err, "while reading file %s", schemaPath)
	}
	// glog.Infof("Setting schema:\n%s\n", data)

	b := bytes.NewBuffer(data)
	resp, err := http.Post(addr+"/schema", "", b)
	if err != nil {
		return errors.Wrapf(err, "while posting schema")
	}
	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "while reading response")
	}
	fmt.Printf("\nSchema update output: %s\n", out)
	return nil
}

type script struct {
	Script string `json:"script"`
}
type uls struct {
	LambdaScript script `json:"lambdaScript"`
}
type d struct {
	Uls uls `json:"updateLambdaScript"`
}
type res struct {
	Data d `json:"data"`
}

func updateLambda(path string) error {
	if len(path) == 0 {
		return nil
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Wrapf(err, "while reading file %s", path)
	}
	sEnc := base64.StdEncoding.EncodeToString(data)

	q := fmt.Sprintf(`mutation {
updateLambdaScript(input: {set: {script: %q }}) {
		lambdaScript { script }
	}}`, sEnc)

	fmt.Printf("Sending query:\n%s\n", q)

	b := bytes.NewBufferString(q)
	resp, err := http.Post(addr, "application/graphql", b)
	if err != nil {
		return errors.Wrapf(err, "while posting lambda")
	}

	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "while reading response")
	}
	var res res
	if err := json.Unmarshal(out, &res); err != nil {
		fmt.Printf("Error while unmarshal of response. Update Failed. Resp: %s\n", out)
		return err
	}
	if res.Data.Uls.LambdaScript.Script == sEnc {
		fmt.Printf("\n Lambda Update: OK\n")
		return nil
	}
	fmt.Printf("\n Lambda Update: FAIL\n")
	return fmt.Errorf("lambda update failed: %s\n", out)
}

func run() {
	addr = Update.Conf.GetString("addr")
	x.AssertTruef(len(addr) > 0, "Outserv server address must be provided")

	schemaPath := Update.Conf.GetString("schema")
	x.Checkf(updateSchema(schemaPath), "Schema update failed")

	lambdaPath := Update.Conf.GetString("lambda")
	x.Checkf(updateLambda(lambdaPath), "Lambda update failed")
}
