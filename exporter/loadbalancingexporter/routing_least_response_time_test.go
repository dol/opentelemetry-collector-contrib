// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
)

func TestLeastResponseTimeExporterSelection(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1", "endpoint-2", "endpoint-3"}}),
		},
		Routing: RoutingSettings{
			Algorithm: leastResponseTimeRoutingAlgorithmStr,
		},
	})

	p.exporters["endpoint-1:4317"].latencyEWMA = 90
	p.exporters["endpoint-1:4317"].latencyInitialized = true
	p.exporters["endpoint-2:4317"].latencyEWMA = 40
	p.exporters["endpoint-2:4317"].latencyInitialized = true
	p.exporters["endpoint-3:4317"].latencyEWMA = 70
	p.exporters["endpoint-3:4317"].latencyInitialized = true

	_, endpoint, err := p.exporterAndEndpoint([]byte("identifier"))
	require.NoError(t, err)
	assert.Equal(t, "endpoint-2", endpoint)
}

func TestLeastResponseTimePrefersObservedLatencyOverUninitializedEndpoints(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1", "endpoint-2"}}),
		},
		Routing: RoutingSettings{
			Algorithm: leastResponseTimeRoutingAlgorithmStr,
		},
	})

	p.exporters["endpoint-1:4317"].latencyEWMA = 15
	p.exporters["endpoint-1:4317"].latencyInitialized = true

	_, endpoint, err := p.exporterAndEndpoint([]byte("identifier"))
	require.NoError(t, err)
	assert.Equal(t, "endpoint-1", endpoint)
}
