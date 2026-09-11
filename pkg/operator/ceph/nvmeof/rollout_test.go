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
	k8sfake "k8s.io/client-go/kubernetes/fake"
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

// deployment returns a single-replica gateway deployment with the given strategy and status.
func deployment(name string, strategy appsv1.DeploymentStrategyType, generation, observed int64, total, updated, ready int32) *appsv1.Deployment {
	one := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "rook-ceph",
			Generation: generation,
			Labels:     map[string]string{"app": AppName, "app.kubernetes.io/part-of": "my-nvmeof"},
		},
		Spec: appsv1.DeploymentSpec{Replicas: &one, Strategy: appsv1.DeploymentStrategy{Type: strategy}},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: observed, Replicas: total, UpdatedReplicas: updated, ReadyReplicas: ready,
		},
	}
}

func deploymentMap(deployments ...*appsv1.Deployment) map[string]*appsv1.Deployment {
	m := map[string]*appsv1.Deployment{}
	for _, d := range deployments {
		m[d.Name] = d
	}
	return m
}

var (
	aRolledOut   = deployment(gwA, appsv1.RecreateDeploymentStrategyType, 5, 5, 1, 1, 1)
	bRolledOut   = deployment(gwB, appsv1.RecreateDeploymentStrategyType, 5, 5, 1, 1, 1)
	cRolledOut   = deployment(gwC, appsv1.RecreateDeploymentStrategyType, 5, 5, 1, 1, 1)
	allRolledOut = deploymentMap(aRolledOut, bRolledOut, cRolledOut)
)

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

func TestDeploymentRolledOut(t *testing.T) {
	assert.True(t, deploymentRolledOut(aRolledOut))
	assert.False(t, deploymentRolledOut(deployment(gwA, appsv1.RecreateDeploymentStrategyType, 6, 5, 1, 1, 1)), "spec not observed yet")
	assert.False(t, deploymentRolledOut(deployment(gwA, appsv1.RecreateDeploymentStrategyType, 6, 6, 0, 0, 0)), "old pod gone, new pod not created")
	assert.False(t, deploymentRolledOut(deployment(gwA, appsv1.RecreateDeploymentStrategyType, 6, 6, 1, 1, 0)), "new pod not ready")
	assert.False(t, deploymentRolledOut(deployment(gwA, appsv1.RollingUpdateDeploymentStrategyType, 6, 6, 2, 1, 1)), "surge pod pending next to the old one")
}

func TestCheckSafeToStop(t *testing.T) {
	converged := gatewayMap(aConverged, bConverged, cConverged)
	tests := []struct {
		name        string
		gwMap       *nvmeofGatewayMap
		deployments map[string]*appsv1.Deployment
		target      string
		wantErr     string
	}{
		{"all converged", converged, allRolledOut, gwB, ""},
		{"other gateway covers a group for the target", gatewayMap(aDown, bCoveringForA, cConverged), allRolledOut, gwB, gwA},
		{"target itself is down", gatewayMap(aDown, bCoveringForA, cConverged), allRolledOut, gwA, ""},
		{"failback still in progress", gatewayMap(aFailingBack, bFailbackToA, cConverged), allRolledOut, gwC, gwA},
		{"target not registered", gatewayMap(aConverged, bConverged), allRolledOut, gwC, ""},
		{"other gateway not registered", gatewayMap(aConverged, bConverged), allRolledOut, gwA, ""},
		{"whole group broken", gatewayMap(aDown, gw(gwB, 2, "UNAVAILABLE", ""), gw(gwC, 3, "UNAVAILABLE", "")), allRolledOut, gwB, ""},
		{
			"previous gateway still restarting although the map has not noticed",
			converged,
			deploymentMap(deployment(gwA, appsv1.RecreateDeploymentStrategyType, 6, 6, 0, 0, 0), bRolledOut, cRolledOut),
			gwB, "deployment \"" + gwA + "\" is still rolling out",
		},
		{
			"previous gateway updated but not observed yet",
			converged,
			deploymentMap(deployment(gwA, appsv1.RecreateDeploymentStrategyType, 6, 5, 1, 1, 1), bRolledOut, cRolledOut),
			gwB, gwA,
		},
		{
			// the state the RollingUpdate port deadlock leaves behind: not yet part of this rollout
			"deployments still stuck on RollingUpdate are judged by the map",
			converged,
			deploymentMap(aRolledOut, deployment(gwB, appsv1.RollingUpdateDeploymentStrategyType, 4, 4, 2, 1, 1), deployment(gwC, appsv1.RollingUpdateDeploymentStrategyType, 2, 2, 2, 1, 1)),
			gwB, "",
		},
		{
			"target not serving is not held back by other rollouts",
			gatewayMap(aDown, bCoveringForA, cConverged),
			deploymentMap(aRolledOut, deployment(gwB, appsv1.RecreateDeploymentStrategyType, 6, 6, 1, 1, 0), cRolledOut),
			gwA, "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkSafeToStop(tt.gwMap, tt.deployments, allGateways, tt.target)
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
	// order, repeating the last one, and a pointer to the number of calls made. The clientset holds
	// the given deployments.
	newReconcile := func(spec cephv1.ClusterSpec, deployments []*appsv1.Deployment, replies ...string) (*ReconcileCephNVMeOFGateway, *int) {
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
		clientset := k8sfake.NewClientset()
		for _, d := range deployments {
			_, err := clientset.AppsV1().Deployments(d.Namespace).Create(context.TODO(), d, metav1.CreateOptions{})
			assert.NoError(t, err)
		}
		return &ReconcileCephNVMeOFGateway{
			context:          &clusterd.Context{Executor: executor, Clientset: clientset},
			clusterInfo:      cephclient.AdminTestClusterInfo("rook-ceph"),
			cephClusterSpec:  &spec,
			opManagerContext: context.TODO(),
		}, &calls
	}
	rolledOut := []*appsv1.Deployment{aRolledOut, bRolledOut, cRolledOut}

	converged := mustJSON(gatewayMap(aConverged, bConverged, cConverged))
	aDownMap := mustJSON(gatewayMap(aDown, bCoveringForA, cConverged))
	aFailbackMap := mustJSON(gatewayMap(aFailingBack, bFailbackToA, cConverged))

	t.Run("stop waits until the other gateways converge", func(t *testing.T) {
		r, calls := newReconcile(cephv1.ClusterSpec{}, rolledOut, aDownMap, aFailbackMap, converged)
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwB)("stop"))
		assert.Equal(t, 3, *calls)
	})

	t.Run("stop waits while another gateway deployment is rolling out", func(t *testing.T) {
		restarting := deployment(gwA, appsv1.RecreateDeploymentStrategyType, 6, 6, 0, 0, 0)
		r, _ := newReconcile(cephv1.ClusterSpec{}, []*appsv1.Deployment{restarting, bRolledOut, cRolledOut}, converged)
		assert.ErrorContains(t, r.gatewayUpdateCallback(nvmeof, gwB)("stop"), "still rolling out")
	})

	t.Run("continue waits for the restarted gateway to fail back", func(t *testing.T) {
		r, calls := newReconcile(cephv1.ClusterSpec{}, rolledOut, aDownMap, aFailbackMap, converged)
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwA)("continue"))
		assert.Equal(t, 3, *calls)
	})

	t.Run("gives up after the timeout", func(t *testing.T) {
		r, _ := newReconcile(cephv1.ClusterSpec{}, rolledOut, aDownMap)
		err := r.gatewayUpdateCallback(nvmeof, gwB)("stop")
		assert.ErrorContains(t, err, "did not converge")
		assert.ErrorContains(t, err, gwA)
	})

	t.Run("unparseable map is retried and reported", func(t *testing.T) {
		r, _ := newReconcile(cephv1.ClusterSpec{}, rolledOut, "not json")
		assert.ErrorContains(t, r.gatewayUpdateCallback(nvmeof, gwA)("continue"), "failed to parse nvme-gw map")
	})

	t.Run("continueUpgradeAfterChecksEvenIfNotHealthy proceeds after the timeout", func(t *testing.T) {
		r, calls := newReconcile(cephv1.ClusterSpec{ContinueUpgradeAfterChecksEvenIfNotHealthy: true}, rolledOut, aDownMap)
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwB)("stop"))
		assert.Greater(t, *calls, 1)
	})

	t.Run("skipUpgradeChecks does not query ceph", func(t *testing.T) {
		r, calls := newReconcile(cephv1.ClusterSpec{SkipUpgradeChecks: true}, rolledOut, aDownMap)
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwB)("stop"))
		assert.NoError(t, r.gatewayUpdateCallback(nvmeof, gwB)("continue"))
		assert.Equal(t, 0, *calls)
	})
}
