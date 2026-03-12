// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
)

func TestPowerOfTwoChoicesExporterSelection(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1", "endpoint-2"}}),
		},
		Routing: RoutingSettings{
			Algorithm: powerOfTwoChoicesRoutingAlgorithmStr,
		},
	})

	p.exporters["endpoint-1:4317"].inflightRequests = 5
	p.exporters["endpoint-2:4317"].inflightRequests = 1

	_, endpoint, err := p.exporterAndEndpoint([]byte("identifier"))
	require.NoError(t, err)
	assert.Equal(t, "endpoint-2", endpoint)
}

func TestPowerOfTwoChoicesFallsBackToStableTieBreak(t *testing.T) {
	p := newRoutingTestLoadBalancer(t, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-2", "endpoint-1"}}),
		},
		Routing: RoutingSettings{
			Algorithm: powerOfTwoChoicesRoutingAlgorithmStr,
		},
	})

	p.exporters["endpoint-1:4317"].inflightRequests = 2
	p.exporters["endpoint-2:4317"].inflightRequests = 2

	_, endpoint, err := p.exporterAndEndpoint([]byte("identifier"))
	require.NoError(t, err)
	assert.Equal(t, "endpoint-1", endpoint)
}
