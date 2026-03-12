// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap/confmaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))
	require.NotNil(t, cfg)
}

func TestLoadConfigWithRoutingAlgorithmAndStructuredStaticEndpoints(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "6").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	lbCfg := cfg.(*Config)
	require.Equal(t, leastConnectionsRoutingAlgorithmStr, lbCfg.Routing.Algorithm)
	require.Equal(t, []StaticEndpoint{
		{Endpoint: "endpoint-1", Weight: 2},
		{Endpoint: "endpoint-2:55678", Weight: 1},
	}, lbCfg.Resolver.Static.Get().Endpoints)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		expectedErr string
	}{
		{
			name: "attribute routing requires routing attributes",
			cfg: Config{
				RoutingKey: attrRoutingStr,
			},
			expectedErr: fmt.Sprintf("routing_attributes must be specified when routing_key is %q", attrRoutingStr),
		},
		{
			name: "attribute routing with routing attributes is valid",
			cfg: Config{
				RoutingKey:        attrRoutingStr,
				RoutingAttributes: []string{"service.name"},
			},
		},
		{
			name: "routing attributes with non attribute routing is invalid",
			cfg: Config{
				RoutingKey:        svcRoutingStr,
				RoutingAttributes: []string{"service.name"},
			},
			expectedErr: fmt.Sprintf(
				"routing_attributes can only be used when routing_key is %q; got %q. Remove routing_attributes or set routing_key to %q",
				attrRoutingStr,
				svcRoutingStr,
				attrRoutingStr,
			),
		},
		{
			name: "routing attributes with empty routing key is invalid",
			cfg: Config{
				RoutingAttributes: []string{"service.name"},
			},
			expectedErr: fmt.Sprintf(
				"routing_attributes can only be used when routing_key is %q; got %q. Remove routing_attributes or set routing_key to %q",
				attrRoutingStr,
				"",
				attrRoutingStr,
			),
		},
		{
			name: "routing algorithm defaults to consistent hashing",
			cfg: Config{
				Resolver: ResolverSettings{
					Static: configoptional.Some(StaticResolver{Hostnames: []string{"backend-1:4317"}}),
				},
			},
		},
		{
			name: "weighted round robin requires structured static endpoints",
			cfg: Config{
				Resolver: ResolverSettings{
					Static: configoptional.Some(StaticResolver{Hostnames: []string{"backend-1:4317"}}),
				},
				Routing: RoutingSettings{
					Algorithm: weightedRoundRobinRoutingAlgorithmStr,
				},
			},
			expectedErr: "routing.algorithm \"weighted_round_robin\" requires resolver.static.endpoints with weights",
		},
		{
			name: "weighted round robin with structured static endpoints is valid",
			cfg: Config{
				Resolver: ResolverSettings{
					Static: configoptional.Some(StaticResolver{
						Endpoints: []StaticEndpoint{
							{Endpoint: "backend-1:4317", Weight: 2},
							{Endpoint: "backend-2:4317", Weight: 1},
						},
					}),
				},
				Routing: RoutingSettings{
					Algorithm: weightedRoundRobinRoutingAlgorithmStr,
				},
			},
		},
		{
			name: "static resolver rejects mixed hostnames and endpoints",
			cfg: Config{
				Resolver: ResolverSettings{
					Static: configoptional.Some(StaticResolver{
						Hostnames: []string{"backend-1:4317"},
						Endpoints: []StaticEndpoint{{Endpoint: "backend-2:4317", Weight: 1}},
					}),
				},
			},
			expectedErr: "resolver.static.hostnames and resolver.static.endpoints are mutually exclusive",
		},
		{
			name: "unknown routing algorithm is invalid",
			cfg: Config{
				Resolver: ResolverSettings{
					Static: configoptional.Some(StaticResolver{Hostnames: []string{"backend-1:4317"}}),
				},
				Routing: RoutingSettings{
					Algorithm: "does_not_exist",
				},
			},
			expectedErr: "unsupported routing.algorithm: \"does_not_exist\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.expectedErr == "" {
				require.NoError(t, err)
				return
			}

			require.EqualError(t, err, tt.expectedErr)
		})
	}
}
