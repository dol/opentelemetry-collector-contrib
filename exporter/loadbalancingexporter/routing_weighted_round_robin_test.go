// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
)

func TestWeightedRoundRobinExporterSelection(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{
				Endpoints: []StaticEndpoint{
					{Endpoint: "endpoint-1", Weight: 2},
					{Endpoint: "endpoint-2", Weight: 1},
				},
			}),
		},
		Routing: RoutingSettings{
			Algorithm: weightedRoundRobinRoutingAlgorithmStr,
		},
	})

	expected := []string{"endpoint-1", "endpoint-1", "endpoint-2", "endpoint-1", "endpoint-1", "endpoint-2"}
	for i, identifier := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")} {
		_, endpoint, err := p.exporterAndEndpoint(identifier)
		require.NoError(t, err)
		assert.Equal(t, expected[i], endpoint)
	}
}

func TestWeightedRoundRobinUsesConfiguredStaticWeights(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{
				Endpoints: []StaticEndpoint{
					{Endpoint: "endpoint-1", Weight: 3},
					{Endpoint: "endpoint-2", Weight: 1},
				},
			}),
		},
		Routing: RoutingSettings{
			Algorithm: weightedRoundRobinRoutingAlgorithmStr,
		},
	})

	assert.Equal(t, int64(3), p.exporters["endpoint-1:4317"].stats().weight)
	assert.Equal(t, int64(1), p.exporters["endpoint-2:4317"].stats().weight)
}
