// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
)

func TestRoundRobinExporterSelection(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1", "endpoint-2", "endpoint-3"}}),
		},
		Routing: RoutingSettings{
			Algorithm: roundRobinRoutingAlgorithmStr,
		},
	})

	expected := []string{"endpoint-1", "endpoint-2", "endpoint-3", "endpoint-1"}
	for i, identifier := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")} {
		_, endpoint, err := p.exporterAndEndpoint(identifier)
		require.NoError(t, err)
		assert.Equal(t, expected[i], endpoint)
	}
}

func TestRoundRobinSkipsUnhealthyEndpoints(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1", "endpoint-2"}}),
		},
		Routing: RoutingSettings{
			Algorithm: roundRobinRoutingAlgorithmStr,
		},
	})

	p.exporters["endpoint-1:4317"].healthy = false

	_, endpoint, err := p.exporterAndEndpoint([]byte("identifier"))
	require.NoError(t, err)
	assert.Equal(t, "endpoint-2", endpoint)
}

func newRoutingTestLoadBalancer(t *testing.T, cfg *Config) *loadBalancer {
	t.Helper()

	ts, tb := getTelemetryAssets(t)
	componentFactory := func(_ context.Context, _ string) (component.Component, error) {
		return newNopMockExporter(), nil
	}

	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(t.Context()))
	})

	return p
}
