// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
)

func TestLeastConnectionsExporterSelection(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1", "endpoint-2", "endpoint-3"}}),
		},
		Routing: RoutingSettings{
			Algorithm: leastConnectionsRoutingAlgorithmStr,
		},
	})

	p.exporters["endpoint-1:4317"].inflightRequests = 4
	p.exporters["endpoint-2:4317"].inflightRequests = 1
	p.exporters["endpoint-3:4317"].inflightRequests = 2

	_, endpoint, err := p.exporterAndEndpoint([]byte("identifier"))
	require.NoError(t, err)
	assert.Equal(t, "endpoint-2", endpoint)
}

func TestLeastConnectionsFallsBackToHealthyOrderingOnTie(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-2", "endpoint-1"}}),
		},
		Routing: RoutingSettings{
			Algorithm: leastConnectionsRoutingAlgorithmStr,
		},
	})

	p.exporters["endpoint-1:4317"].inflightRequests = 1
	p.exporters["endpoint-2:4317"].inflightRequests = 1
	p.exporters["endpoint-1:4317"].healthy = true
	p.exporters["endpoint-2:4317"].healthy = true

	_, endpoint, err := p.exporterAndEndpoint([]byte("identifier"))
	require.NoError(t, err)
	assert.Equal(t, "endpoint-1", endpoint)
}
