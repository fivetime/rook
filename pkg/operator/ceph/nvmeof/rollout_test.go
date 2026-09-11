/*
Copyright 2026 The Rook Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nvmeof

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	exectest "github.com/rook/rook/pkg/util/exec/test"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// stubUpdateDeploymentAndWait replaces updateDeploymentAndWait for the duration of the test and
// returns the names of the deployments it was called for, in order.
func stubUpdateDeploymentAndWait(t *testing.T) *[]string {
	t.Helper()
	updated := &[]string{}
	old := updateDeploymentAndWait
	updateDeploymentAndWait = func(_ context.Context, _ *clusterd.Context, d *appsv1.Deployment, _ string, _ func(string) error) error {
		*updated = append(*updated, d.Name)
		return nil
	}
	t.Cleanup(func() { updateDeploymentAndWait = old })
	return updated
}

// Output of `ceph nvme-gw show nvmeof-rbd-pool group-a --format json` on Ceph v20.2.4 with three
// healthy gateways, names changed to match the test CR.
const convergedGatewayMap = `{"epoch":1390,"pool":"nvmeof-rbd-pool","group":"group-a","beacon_diff_enabled":true,"features":"LB","rebalance_ana_group":3,"num gws":3,"GW-epoch":510,"Anagrp list":"[ 1 2 3 ]","num-namespaces":0,"Created Gateways:":[{"gw-id":"rook-ceph-nvmeof-my-nvmeof-a","anagrp-id":1,"location":"","admin state":"ENABLED","num-namespaces":0,"performed-full-startup":1,"Availability":"AVAILABLE","num-listeners":1,"ana states":" 1: ACTIVE ,  2: STANDBY ,  3: STANDBY "},{"gw-id":"rook-ceph-nvmeof-my-nvmeof-b","anagrp-id":2,"location":"","admin state":"ENABLED","num-namespaces":0,"performed-full-startup":1,"Availability":"AVAILABLE","num-listeners":1,"ana states":" 1: STANDBY ,  2: ACTIVE ,  3: STANDBY "},{"gw-id":"rook-ceph-nvmeof-my-nvmeof-c","anagrp-id":3,"location":"","admin state":"ENABLED","num-namespaces":0,"performed-full-startup":1,"Availability":"AVAILABLE","num-listeners":1,"ana states":" 1: STANDBY ,  2: STANDBY ,  3: ACTIVE "}]}`

const (
	gwA = "rook-ceph-nvmeof-my-nvmeof-a"
	gwB = "rook-ceph-nvmeof-my-nvmeof-b"
	gwC = "rook-ceph-nvmeof-my-nvmeof-c"
)

var allGateways = []string{gwA, gwB, gwC}

func gw(id string, group int, availability, anaStates string) nvmeofGatewayState {
	return nvmeofGatewayState{ID: id, AnaGroupID: group, Availability: availability, AnaStates: anaStates}
}

var (
	aConverged = gw(gwA, 1, "AVAILABLE", " 1: ACTIVE ,  2: STANDBY ,  3: STANDBY ")
	bConverged = gw(gwB, 2, "AVAILABLE", " 1: STANDBY ,  2: ACTIVE ,  3: STANDBY ")
	cConverged = gw(gwC, 3, "AVAILABLE", " 1: STANDBY ,  2: STANDBY ,  3: ACTIVE ")
	// a is down and b serves group 1 for it
	aDown         = gw(gwA, 1, "UNAVAILABLE", " 1: STANDBY ,  2: STANDBY ,  3: STANDBY ")
	bCoveringForA = gw(gwB, 2, "AVAILABLE", " 1: ACTIVE ,  2: ACTIVE ,  3: STANDBY ")
	// a is back but group 1 has not failed back from b yet
	aFailingBack = gw(gwA, 1, "AVAILABLE", " 1: OWNER_WAIT_FAILBACK_PREPARED ,  2: STANDBY ,  3: STANDBY ")
	bFailbackToA = gw(gwB, 2, "AVAILABLE", " 1: WAIT_FAILBACK_PREPARED ,  2: ACTIVE ,  3: STANDBY ")
)

func gatewayMap(gateways ...nvmeofGatewayState) *nvmeofGatewayMap {
	return &nvmeofGatewayMap{Gateways: gateways}
}

func TestParseGatewayMap(t *testing.T) {
	gwMap := &nvmeofGatewayMap{}
	assert.NoError(t, json.Unmarshal([]byte(convergedGatewayMap), gwMap))
	assert.Equal(t, []nvmeofGatewayState{aConverged, bConverged, cConverged}, gwMap.Gateways)

	assert.Equal(t, "ACTIVE", aConverged.anaState(1))
	assert.Equal(t, "STANDBY", aConverged.anaState(3))
	assert.Equal(t, "", aConverged.anaState(4))
	assert.Equal(t, "OWNER_WAIT_FAILBACK_PREPARED", aFailingBack.anaState(1))
	assert.Equal(t, "", gw(gwA, 1, "CREATED", "").anaState(1))

	assert.True(t, aConverged.converged())
	assert.False(t, aDown.converged())
	assert.False(t, aFailingBack.converged())
	// b still owns its own group; the pending failback of group 1 shows up on a
	assert.True(t, bFailbackToA.converged())
}

func TestCheckSafeToStop(t *testing.T) {
	tests := []struct {
		name    string
		gwMap   *nvmeofGatewayMap
		target  string
		wantErr string
	}{
		{"all converged", gatewayMap(aConverged, bConverged, cConverged), gwB, ""},
		{"other gateway covers a group for the target", gatewayMap(aDown, bCoveringForA, cConverged), gwB, gwA},
		{"target itself is down", gatewayMap(aDown, bCoveringForA, cConverged), gwA, ""},
		{"failback still in progress", gatewayMap(aFailingBack, bFailbackToA, cConverged), gwC, gwA},
		{"target not registered", gatewayMap(aConverged, bConverged), gwC, ""},
		{"other gateway not registered", gatewayMap(aConverged, bConverged), gwA, ""},
		{"whole group broken", gatewayMap(aDown, gw(gwB, 2, "UNAVAILABLE", ""), gw(gwC, 3, "UNAVAILABLE", "")), gwB, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkSafeToStop(tt.gwMap, allGateways, tt.target)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestCheckRestarted(t *testing.T) {
	tests := []struct {
		name    string
		gwMap   *nvmeofGatewayMap
		target  string
		wantErr string
	}{
		{"all converged", gatewayMap(aConverged, bConverged, cConverged), gwA, ""},
		{"target not registered yet", gatewayMap(bCoveringForA, cConverged), gwA, "not in the gateway map"},
		{"target still down", gatewayMap(aDown, bCoveringForA, cConverged), gwA, gwA},
		{"failback still in progress", gatewayMap(aFailingBack, bFailbackToA, cConverged), gwA, gwA},
		{"another gateway went down meanwhile", gatewayMap(aConverged, bConverged, gw(gwC, 3, "UNAVAILABLE", "")), gwA, gwC},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkRestarted(tt.gwMap, allGateways, tt.target)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestGatewayUpdateCallback(t *testing.T) {
	oldTimeout, oldInterval := gatewayConvergeTimeout, gatewayConvergeInterval
	gatewayConvergeTimeout, gatewayConvergeInterval = 200*time.Millisecond, time.Millisecond
	t.Cleanup(func() { gatewayConvergeTimeout, gatewayConvergeInterval = oldTimeout, oldInterval })

	nvmeof := &cephv1.CephNVMeOFGateway{
		ObjectMeta: metav1.ObjectMeta{Name: "my-nvmeof", Namespace: "rook-ceph"},
		Spec:       cephv1.NVMeOFGatewaySpec{Pool: "nvmeof", Group: "group-a", Instances: 3},
	}

	mustJSON := func(m *nvmeofGatewayMap) string {
		b, err := json.Marshal(m)
		assert.NoError(t, err)
		return string(b)
	}

	// newReconcile returns a reconciler whose `ceph nvme-gw show` replies with the given maps in
	// order, repeating the last one, and a pointer to the number of calls made.
	newReconcile := func(spec cephv1.ClusterSpec, replies ...string) (*ReconcileCephNVMeOFGateway, *int) {
		calls := 0
		executor := &exectest.MockExecutor{
			MockExecuteCommandWithOutput: func(command string, args ...string) (string, error) {
				if command != "ceph" || args[0] != "nvme-gw" || args[1] != "show" || args[2] != "nvmeof" || args[3] != "group-a" {
					panic(fmt.Sprintf("unhandled command %s %v", command, args))
				}
				reply := replies[min(calls, len(replies)-1)]
				calls++
				return reply, nil
			},
		}
		return &ReconcileCephNVMeOFGateway{
			context:          &clusterd.Context{Executor: executor},
			clusterInfo:      cephclient.AdminTestClusterInfo("rook-ceph"),
			cephClusterSpec:  &spec,
			opManagerContext: context.TODO(),
		}, &calls
	}

	converged := mustJSON(gatewayMap(aConverged, bConverged, cConverged))
	aDownMap := mustJSON(gatewayMap(aDown, bCoveringForA, cConverged))
	aFailbackMap := mustJSON(gatewayMap(aFailingBack, bFailbackToA, cConverged))

	t.Run("stop waits until the other gateways converge", func(t *testing.T) {
		r, calls := newReconcile(cephv1.ClusterSpec{}, aDownMap, aFailbackMap, converged)
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwB)("stop"))
		assert.Equal(t, 3, *calls)
	})

	t.Run("continue waits for the restarted gateway to fail back", func(t *testing.T) {
		r, calls := newReconcile(cephv1.ClusterSpec{}, aDownMap, aFailbackMap, converged)
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwA)("continue"))
		assert.Equal(t, 3, *calls)
	})

	t.Run("gives up after the timeout", func(t *testing.T) {
		r, _ := newReconcile(cephv1.ClusterSpec{}, aDownMap)
		err := r.gatewayUpdateCallback(nvmeof, gwB)("stop")
		assert.ErrorContains(t, err, "did not converge")
		assert.ErrorContains(t, err, gwA)
	})

	t.Run("unparseable map is retried and reported", func(t *testing.T) {
		r, _ := newReconcile(cephv1.ClusterSpec{}, "not json")
		assert.ErrorContains(t, r.gatewayUpdateCallback(nvmeof, gwA)("continue"), "failed to parse nvme-gw map")
	})

	t.Run("continueUpgradeAfterChecksEvenIfNotHealthy proceeds after the timeout", func(t *testing.T) {
		r, calls := newReconcile(cephv1.ClusterSpec{ContinueUpgradeAfterChecksEvenIfNotHealthy: true}, aDownMap)
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwB)("stop"))
		assert.Greater(t, *calls, 1)
	})

	t.Run("skipUpgradeChecks does not query ceph", func(t *testing.T) {
		r, calls := newReconcile(cephv1.ClusterSpec{SkipUpgradeChecks: true}, aDownMap)
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwB)("stop"))
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwB)("continue"))
		assert.Equal(t, 0, *calls)
	})
}
