/*
Copyright 2026.

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

package controller

// Shared fixture identifiers for the controller tests, kept in one place so the same cluster,
// bucket and pool names can be reused across test files without repeating string literals.
const (
	testClusterName = "homelab"
	testClusterNS   = "storage"
	testBucketName  = "photos"
	testBucketNS    = "media"
	testBucketID    = "bucket-1"
	testPoolName    = "default"
	testKeyName     = "photos-rw"
	testKeyNS       = "media"
	testGarageKeyID = "GK-1"

	// reasonClusterReady is the condition reason the test fixtures stamp on a Ready cluster.
	reasonClusterReady = "ClusterReady"
)
