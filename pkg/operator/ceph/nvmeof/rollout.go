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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/operator/k8sutil"
	appsv1 "k8s.io/api/apps/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	gatewayAvailable = "AVAILABLE"
	anaStateActive   = "ACTIVE"
)

var (
	// updateDeploymentAndWait is a variable so unit tests can replace it: the fake clientset never
	// reports an updated deployment as ready.
	updateDeploymentAndWait = k8sutil.UpdateDeploymentAndWait

	gatewayConvergeTimeout  = 5 * time.Minute
	gatewayConvergeInterval = 5 * time.Second
)

// nvmeofGatewayMap is the part of `ceph nvme-gw show <pool> <group>` used to decide whether a
// gateway can be restarted without leaving an ANA group with no active path.
type nvmeofGatewayMap struct {
	Gateways []nvmeofGatewayState `json:"Created Gateways:"`
}

type nvmeofGatewayState struct {
	ID           string `json:"gw-id"`
	AnaGroupID   int    `json:"anagrp-id"`
	Availability string `json:"Availability"`
	// Formatted by the mon as " 1: ACTIVE ,  2: STANDBY ,  3: STANDBY ".
	AnaStates string `json:"ana states"`
}

// anaState returns the state the gateway reports for the given ANA group, or "" if it reports none.
func (g nvmeofGatewayState) anaState(group int) string {
	for _, entry := range strings.Split(g.AnaStates, ",") {
		id, state, found := strings.Cut(entry, ":")
		if !found {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimSpace(id)); err == nil && n == group {
			return strings.TrimSpace(state)
		}
	}
	return ""
}

// converged means the gateway is available and has failed its own ANA group back, so no other
// gateway is still serving that group on its behalf.
func (g nvmeofGatewayState) converged() bool {
	return g.Availability == gatewayAvailable && g.anaState(g.AnaGroupID) == anaStateActive
}

func (m *nvmeofGatewayMap) find(name string) (nvmeofGatewayState, bool) {
	for _, gw := range m.Gateways {
		if gw.ID == name {
			return gw, true
		}
	}
	return nvmeofGatewayState{}, false
}

// deploymentRolledOut is true once the deployment controller has acted on the latest spec and
// every replica is updated and ready.
func deploymentRolledOut(d *appsv1.Deployment) bool {
	replicas := int32(1)
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	return d.Status.ObservedGeneration >= d.Generation &&
		d.Status.Replicas == replicas &&
		d.Status.UpdatedReplicas == replicas &&
		d.Status.ReadyReplicas == replicas
}

// checkSafeToStop returns nil if restarting the target gateway cannot take away the last active
// path of any ANA group.
//
// A gateway that is not serving can always be restarted; this is also what lets a configuration
// fix roll out when every gateway in the group is broken.
//
// The gateway map alone is not enough right after another gateway was updated: the mon keeps
// reporting a restarting gateway as available until its beacons time out, and
// UpdateDeploymentAndWait returns early for a deployment whose Progressing condition still says
// ProgressDeadlineExceeded -- the state the RollingUpdate port deadlock leaves gateways in. So a
// deployment already switched to Recreate must also have finished rolling out. Deployments still
// on the old strategy have not been touched by this rollout and are judged by the map alone.
func checkSafeToStop(gwMap *nvmeofGatewayMap, deployments map[string]*appsv1.Deployment, gateways []string, target string) error {
	if gw, ok := gwMap.find(target); !ok || gw.Availability != gatewayAvailable {
		return nil
	}
	for _, name := range gateways {
		if name == target {
			continue
		}
		if d, ok := deployments[name]; ok && d.Spec.Strategy.Type == appsv1.RecreateDeploymentStrategyType && !deploymentRolledOut(d) {
			return errors.Errorf("deployment %q is still rolling out (updated %d, ready %d, total %d)",
				name, d.Status.UpdatedReplicas, d.Status.ReadyReplicas, d.Status.Replicas)
		}
		// A gateway that never registered has no ANA group that could depend on the target.
		if gw, ok := gwMap.find(name); ok && !gw.converged() {
			return errors.Errorf("gateway %q is %s with ANA states %q", name, gw.Availability, gw.AnaStates)
		}
	}
	return nil
}

// checkRestarted returns nil once the restarted gateway is back in the map and every gateway owns
// its ANA group again.
func checkRestarted(gwMap *nvmeofGatewayMap, gateways []string, target string) error {
	if _, ok := gwMap.find(target); !ok {
		return errors.Errorf("gateway %q is not in the gateway map yet", target)
	}
	for _, name := range gateways {
		if gw, ok := gwMap.find(name); ok && !gw.converged() {
			return errors.Errorf("gateway %q is %s with ANA states %q", name, gw.Availability, gw.AnaStates)
		}
	}
	return nil
}

func (r *ReconcileCephNVMeOFGateway) getGatewayMap(nvmeof *cephv1.CephNVMeOFGateway) (*nvmeofGatewayMap, error) {
	args := []string{"nvme-gw", "show", nvmeof.Spec.Pool, nvmeof.Spec.Group}
	buf, err := cephclient.NewCephCommand(r.context, r.clusterInfo, args).Run()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to show nvme-gw map for pool %q group %q", nvmeof.Spec.Pool, nvmeof.Spec.Group)
	}
	gwMap := &nvmeofGatewayMap{}
	if err := json.Unmarshal(buf, gwMap); err != nil {
		return nil, errors.Wrapf(err, "failed to parse nvme-gw map %q", string(buf))
	}
	return gwMap, nil
}

func (r *ReconcileCephNVMeOFGateway) getGatewayDeployments(nvmeof *cephv1.CephNVMeOFGateway) (map[string]*appsv1.Deployment, error) {
	list, err := r.context.Clientset.AppsV1().Deployments(nvmeof.Namespace).List(r.opManagerContext,
		metav1.ListOptions{LabelSelector: gatewayDeploymentSelector(nvmeof)})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list nvmeof gateway deployments")
	}
	deployments := make(map[string]*appsv1.Deployment, len(list.Items))
	for i := range list.Items {
		deployments[list.Items[i].Name] = &list.Items[i]
	}
	return deployments, nil
}

// waitForGateways polls check until it passes or gatewayConvergeTimeout expires.
func (r *ReconcileCephNVMeOFGateway) waitForGateways(target string, check func() error) error {
	deadline := time.Now().Add(gatewayConvergeTimeout)
	for {
		err := check()
		if err == nil {
			return nil
		}
		if !time.Now().Before(deadline) {
			return errors.Wrapf(err, "nvmeof gateways did not converge within %s", gatewayConvergeTimeout)
		}
		logger.Infof("waiting before updating nvmeof gateway %q: %v", target, err)
		select {
		case <-r.opManagerContext.Done():
			return r.opManagerContext.Err()
		case <-time.After(gatewayConvergeInterval):
		}
	}
}

// gatewayUpdateCallback is the verifyCallback for k8sutil.UpdateDeploymentAndWait. Gateways are
// updated one at a time: a gateway is only stopped while the others can take over its ANA group,
// and the next one waits until the restarted gateway has failed its group back.
func (r *ReconcileCephNVMeOFGateway) gatewayUpdateCallback(nvmeof *cephv1.CephNVMeOFGateway, gatewayName string) func(action string) error {
	return func(action string) error {
		if r.cephClusterSpec.SkipUpgradeChecks {
			logger.Warningf("not checking nvmeof gateways before %s of %q because skipUpgradeChecks is set", action, gatewayName)
			return nil
		}

		gateways := gatewayNames(nvmeof)
		var err error
		switch action {
		case "stop":
			err = r.waitForGateways(gatewayName, func() error {
				gwMap, err := r.getGatewayMap(nvmeof)
				if err != nil {
					return err
				}
				deployments, err := r.getGatewayDeployments(nvmeof)
				if err != nil {
					return err
				}
				return checkSafeToStop(gwMap, deployments, gateways, gatewayName)
			})
		case "continue":
			err = r.waitForGateways(gatewayName, func() error {
				gwMap, err := r.getGatewayMap(nvmeof)
				if err != nil {
					return err
				}
				return checkRestarted(gwMap, gateways, gatewayName)
			})
		}
		if err != nil && r.cephClusterSpec.ContinueUpgradeAfterChecksEvenIfNotHealthy {
			logger.Warningf("nvmeof gateway check before %s of %q failed, continuing because continueUpgradeAfterChecksEvenIfNotHealthy is set: %v", action, gatewayName, err)
			return nil
		}
		return err
	}
}

func (r *ReconcileCephNVMeOFGateway) createOrUpdateDeployment(nvmeof *cephv1.CephNVMeOFGateway, deployment *appsv1.Deployment) error {
	_, err := k8sutil.CreateDeployment(r.opManagerContext, r.context.Clientset, deployment)
	if err == nil || !kerrors.IsAlreadyExists(err) {
		return err
	}
	return updateDeploymentAndWait(r.opManagerContext, r.context, deployment, nvmeof.Namespace,
		r.gatewayUpdateCallback(nvmeof, deployment.Name))
}

func gatewayDeploymentSelector(nvmeof *cephv1.CephNVMeOFGateway) string {
	return fmt.Sprintf("%s=%s,app.kubernetes.io/part-of=%s", k8sutil.AppAttr, AppName, nvmeof.Name)
}

func gatewayNames(nvmeof *cephv1.CephNVMeOFGateway) []string {
	names := make([]string, 0, nvmeof.Spec.Instances)
	for i := 0; i < nvmeof.Spec.Instances; i++ {
		names = append(names, instanceName(nvmeof, k8sutil.IndexToName(i)))
	}
	return names
}
