// Portions Copyright 2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package telemetry

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"runtime"
	"time"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
)

// Telemetry holds information about the state of the zero and alpha server.
type Telemetry struct {
	Arch         string `json:",omitempty"`
	Cid          string `json:",omitempty"`
	DiskUsageMB  int64  `json:",omitempty"`
	NumMembers   int    `json:",omitempty"`
	NumGroups    int    `json:",omitempty"`
	NumTablets   int    `json:",omitempty"`
	OS           string `json:",omitempty"`
	SinceHours   int    `json:",omitempty"`
	Version      string `json:",omitempty"`
	NumGraphQLPM uint64 `json:",omitempty"`
	NumGraphQL   uint64 `json:",omitempty"`
}

const url = ""

// NewZero returns a Telemetry struct that holds information about the state of zero server.
func NewZero(ms *pb.MembershipState) *Telemetry {
	if len(ms.GetCid()) == 0 {
		glog.V(2).Infoln("No CID found yet")
		return nil
	}
	t := &Telemetry{
		Cid:        ms.GetCid(),
		NumMembers: len(ms.GetMembers()),
		Version:    x.Version(),
		OS:         runtime.GOOS,
		Arch:       runtime.GOARCH,
	}
	for _, tablet := range ms.GetTablets() {
		t.NumTablets++
		t.DiskUsageMB += tablet.GetOnDiskBytes()
	}
	t.DiskUsageMB /= (1 << 20)
	return t
}

// NewAlpha returns a Telemetry struct that holds information about the state of alpha server.
func NewAlpha(ms *pb.MembershipState) *Telemetry {
	return &Telemetry{
		Cid:     ms.GetCid(),
		Version: x.Version(),
		OS:      runtime.GOOS,
		Arch:    runtime.GOARCH,
	}
}

// Post reports the Telemetry to the stats server.
func (t *Telemetry) Post() error {
	// TODO: Reactivate Telemetry

	return nil
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}

	var requestURL string
	if len(t.Version) > 0 {
		requestURL = url + "/pings"
	} else {
		requestURL = url + "/dev"
	}
	req, err := http.NewRequest("POST", requestURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 201 {
		return errors.Errorf(string(body))
	}
	glog.V(2).Infof("Telemetry response status: %v", resp.Status)
	glog.V(2).Infof("Telemetry response body: %s", body)
	return nil
}
