/*
Copyright 2025 The Rook Authors. All rights reserved.

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
	"encoding/pem"
	"strings"
	"testing"

	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testGateway() *cephv1.CephNVMeOFGateway {
	return &cephv1.CephNVMeOFGateway{
		ObjectMeta: metav1.ObjectMeta{Name: "my-nvmeof", Namespace: "rook-ceph-test-ns"},
		Spec: cephv1.NVMeOFGatewaySpec{
			Image: "quay.io/ceph/nvmeof:1.5", Pool: "nvmeof", Group: "group-a", Instances: 2,
		},
	}
}

// The framing GatewayUtilsCrypto.read_encryption_key accepts: lines are stripped and joined,
// then a "-----BEGIN <label>-----" ... "-----END <label>-----" pair with equal labels must wrap
// a base64 body of at least 32 bytes.
func TestGenerateEncryptionKey(t *testing.T) {
	key, err := generateEncryptionKey()
	require.NoError(t, err)

	joined := ""
	for _, line := range strings.Split(string(key), "\n") {
		joined += strings.TrimSpace(line)
	}
	assert.True(t, strings.HasPrefix(joined, "-----BEGIN NVMEOF ENCRYPTION KEY-----"))
	assert.True(t, strings.HasSuffix(joined, "-----END NVMEOF ENCRYPTION KEY-----"))

	block, rest := pem.Decode(key)
	require.NotNil(t, block)
	assert.Empty(t, strings.TrimSpace(string(rest)))
	assert.Len(t, block.Bytes, encryptionKeyBytes)

	other, err := generateEncryptionKey()
	require.NoError(t, err)
	assert.NotEqual(t, key, other)
}

func TestEnsureEncryptionKeySecret(t *testing.T) {
	r, _ := newDeploymentSpecTest(t)
	r.opManagerContext = context.TODO()
	nvmeof := testGateway()
	secrets := r.context.Clientset.CoreV1().Secrets(nvmeof.Namespace)

	require.NoError(t, r.ensureEncryptionKeySecret(nvmeof))
	s, err := secrets.Get(context.TODO(), "rook-ceph-nvmeof-my-nvmeof-encryption-key", metav1.GetOptions{})
	require.NoError(t, err)
	first := s.Data[encryptionKeySecretKey]
	require.NotEmpty(t, first)
	// The OMAP state outlives the CR, so the key must too.
	assert.Empty(t, s.OwnerReferences)

	t.Run("never replaced", func(t *testing.T) {
		require.NoError(t, r.ensureEncryptionKeySecret(nvmeof))
		s, err := secrets.Get(context.TODO(), s.Name, metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, first, s.Data[encryptionKeySecretKey])
	})

	t.Run("an existing key is kept as is", func(t *testing.T) {
		s.Data[encryptionKeySecretKey] = []byte("operator supplied")
		_, err := secrets.Update(context.TODO(), s, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.NoError(t, r.ensureEncryptionKeySecret(nvmeof))
		got, err := secrets.Get(context.TODO(), s.Name, metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, []byte("operator supplied"), got.Data[encryptionKeySecretKey])
	})
}

func TestDeploymentMountsEncryptionKey(t *testing.T) {
	r, configHash := newDeploymentSpecTest(t)
	nvmeof := testGateway()
	d, err := r.makeDeployment(nvmeof, "0", "rook-ceph-nvmeof-my-nvmeof-config", configHash)
	require.NoError(t, err)
	spec := d.Spec.Template.Spec

	var vol *v1.Volume
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == encryptionKeyVolName {
			vol = &spec.Volumes[i]
		}
	}
	require.NotNil(t, vol)
	require.NotNil(t, vol.Secret)
	assert.Equal(t, "rook-ceph-nvmeof-my-nvmeof-encryption-key", vol.Secret.SecretName)
	require.Len(t, vol.Secret.Items, 1)
	assert.Equal(t, int32(0o400), *vol.Secret.Items[0].Mode)

	require.Len(t, spec.InitContainers, 1)
	found := false
	for _, m := range spec.InitContainers[0].VolumeMounts {
		if m.Name == encryptionKeyVolName {
			found = true
			assert.Equal(t, encryptionKeyMountDir, m.MountPath)
			assert.True(t, m.ReadOnly)
		}
	}
	assert.True(t, found, "init container does not mount the encryption key")

	// The init script copies it to the path the generated nvmeof.conf names.
	assert.Contains(t, connectionConfigScript,
		"cp "+encryptionKeyMountDir+"/"+encryptionKeySecretKey+" /etc/ceph/encryption.key")
	conf, err := getNVMeOFGatewayConfig("nvmeof", "gw", "10.0.0.1", "group-a", nil)
	require.NoError(t, err)
	assert.Contains(t, conf, "/etc/ceph/encryption.key")

	// Two instances of one CR share one OMAP state, so they must share the key.
	d1, err := r.makeDeployment(nvmeof, "1", "rook-ceph-nvmeof-my-nvmeof-config", configHash)
	require.NoError(t, err)
	for _, v := range d1.Spec.Template.Spec.Volumes {
		if v.Name == encryptionKeyVolName {
			assert.Equal(t, vol.Secret.SecretName, v.Secret.SecretName)
		}
	}
}
