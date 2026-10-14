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
	"crypto/rand"
	"encoding/pem"
	"fmt"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The gateway encrypts the DH-HMAC-CHAP and PSK keys it stores in its OMAP state with the key in
// [gateway] encryption_key. Without that file it refuses every host key with ENOKEY, so in-band
// authentication cannot be used at all.
const (
	encryptionKeySecretKey = "encryption.key"
	encryptionKeyVolName   = "nvmeof-encryption-key"
	encryptionKeyMountDir  = "/tmp/nvmeof-encryption"
	// The gateway uses the first 32 bytes; it accepts more.
	encryptionKeyBytes = 32
)

func encryptionKeySecretName(nvmeof *cephv1.CephNVMeOFGateway) string {
	return fmt.Sprintf("%s-%s-encryption-key", AppName, nvmeof.Name)
}

// generateEncryptionKey returns a key in the PEM-style framing the gateway's
// GatewayUtilsCrypto.read_encryption_key requires: a BEGIN/END marker pair with matching labels
// around the base64 key.
func generateEncryptionKey() ([]byte, error) {
	b := make([]byte, encryptionKeyBytes)
	if _, err := rand.Read(b); err != nil {
		return nil, errors.Wrap(err, "failed to generate nvmeof encryption key")
	}
	return pem.EncodeToMemory(&pem.Block{Type: "NVMEOF ENCRYPTION KEY", Bytes: b}), nil
}

// ensureEncryptionKeySecret creates the gateways' encryption key once and never changes it: every
// host key already stored would become undecryptable. All instances of a CR share it because they
// share one OMAP state.
//
// The Secret deliberately has no owner reference. The OMAP state outlives the CR in the pool, so a
// CR deleted and created again must find the same key, or its hosts come back without their keys.
func (r *ReconcileCephNVMeOFGateway) ensureEncryptionKeySecret(nvmeof *cephv1.CephNVMeOFGateway) error {
	name := encryptionKeySecretName(nvmeof)
	secrets := r.context.Clientset.CoreV1().Secrets(nvmeof.Namespace)
	if _, err := secrets.Get(r.opManagerContext, name, metav1.GetOptions{}); err == nil {
		return nil
	} else if !kerrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to get secret %q", name)
	}

	key, err := generateEncryptionKey()
	if err != nil {
		return err
	}
	s := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: nvmeof.Namespace,
			Labels:    map[string]string{"app": AppName, "ceph_nvmeof": nvmeof.Name},
		},
		Type: v1.SecretTypeOpaque,
		Data: map[string][]byte{encryptionKeySecretKey: key},
	}
	if _, err := secrets.Create(r.opManagerContext, s, metav1.CreateOptions{}); err != nil {
		// Another reconcile got there first; its key is the one to keep.
		if kerrors.IsAlreadyExists(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to create secret %q", name)
	}
	logger.Infof("created nvmeof gateway encryption key secret %q", name)
	return nil
}

// encryptionKeyVolumeAndMount mounts the key into the init container, which copies it next to
// nvmeof.conf like it does the keyring.
func encryptionKeyVolumeAndMount(nvmeof *cephv1.CephNVMeOFGateway) (v1.Volume, v1.VolumeMount) {
	mode := int32(0o400)
	v := v1.Volume{
		Name: encryptionKeyVolName,
		VolumeSource: v1.VolumeSource{
			// #nosec G101 -- this is a Kubernetes Secret reference, not a credential value
			Secret: &v1.SecretVolumeSource{
				SecretName: encryptionKeySecretName(nvmeof),
				Items: []v1.KeyToPath{
					{Key: encryptionKeySecretKey, Path: encryptionKeySecretKey, Mode: &mode},
				},
			},
		},
	}
	m := v1.VolumeMount{Name: encryptionKeyVolName, MountPath: encryptionKeyMountDir, ReadOnly: true}
	return v, m
}
