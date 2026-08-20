// Copyright 2026 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package restmapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestResourceFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		gvk     schema.GroupVersionKind
		wantGVR schema.GroupVersionResource
	}{
		{
			name:    "ConfigMap from library-go defaults",
			gvk:     schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			wantGVR: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"},
		},
		{
			name:    "Secret from library-go defaults",
			gvk:     schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Secret"},
			wantGVR: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"},
		},
		{
			name:    "Deployment from library-go defaults",
			gvk:     schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			wantGVR: schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		},
		{
			name:    "Namespace from additional mappings",
			gvk:     schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"},
			wantGVR: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"},
		},
		{
			name:    "Ingress from additional mappings",
			gvk:     schema.GroupVersionKind{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"},
			wantGVR: schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
		},
		{
			name:    "NetworkPolicy from additional mappings",
			gvk:     schema.GroupVersionKind{Group: "networking.k8s.io", Version: "v1", Kind: "NetworkPolicy"},
			wantGVR: schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"},
		},
		{
			name:    "Service from additional mappings",
			gvk:     schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"},
			wantGVR: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gvr, err := ResourceFor(tt.gvk)
			require.NoError(t, err, "ResourceFor should succeed for known type")
			assert.Equal(t, tt.wantGVR, gvr)
		})
	}
}

func TestResourceForUnknownType(t *testing.T) {
	t.Parallel()
	_, err := ResourceFor(schema.GroupVersionKind{Group: "fake.example.com", Version: "v1", Kind: "Unknown"})
	assert.Error(t, err, "ResourceFor should error for unknown type")
}
