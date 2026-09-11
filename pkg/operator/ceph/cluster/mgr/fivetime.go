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

package mgr

import "strings"

// fivetimeCephImage is the repository of the ceph overlay images built from the
// v20.2.4-fivetime branch of fivetime/ceph. Those images carry the Python half of
// ceph/ceph#71041: mgr/prometheus no longer calls node_proxy_fullreport() on an
// orchestrator that does not implement it. On v20.2.3 and v20.2.4 that call raises
// NotImplementedError inside the rook module on every scrape and files a mgr crash,
// which is why the rook module is force-disabled for those versions.
const fivetimeCephImage = "ghcr.io/fivetime/ceph"

// cephImageHasRookModuleCrashFix reports whether image is known to carry that fix, by tag
// or by digest.
func cephImageHasRookModuleCrashFix(image string) bool {
	return strings.HasPrefix(image, fivetimeCephImage+":") || strings.HasPrefix(image, fivetimeCephImage+"@")
}
