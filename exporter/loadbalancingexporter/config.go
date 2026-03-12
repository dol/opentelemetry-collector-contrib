// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
)

type routingKey int

const (
	traceIDRouting routingKey = iota
	svcRouting
	metricNameRouting
	resourceRouting
	streamIDRouting
	attrRouting
)

const (
	svcRoutingStr        = "service"
	traceIDRoutingStr    = "traceID"
	metricNameRoutingStr = "metric"
	resourceRoutingStr   = "resource"
	streamIDRoutingStr   = "streamID"
	attrRoutingStr       = "attributes"
)

const (
	consistentHashingRoutingAlgorithmStr  = "consistent_hashing"
	roundRobinRoutingAlgorithmStr         = "round_robin"
	weightedRoundRobinRoutingAlgorithmStr = "weighted_round_robin"
	leastConnectionsRoutingAlgorithmStr   = "least_connections"
	leastResponseTimeRoutingAlgorithmStr  = "least_response_time"
	powerOfTwoChoicesRoutingAlgorithmStr  = "power_of_two_choices"
)

// Config defines configuration for the exporter.
type Config struct {
	TimeoutSettings           exporterhelper.TimeoutConfig `mapstructure:",squash"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	QueueSettings             configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	Protocol Protocol         `mapstructure:"protocol"`
	Resolver ResolverSettings `mapstructure:"resolver"`
	Routing  RoutingSettings  `mapstructure:"routing"`

	// RoutingKey is a single routing key value
	RoutingKey string `mapstructure:"routing_key"`

	// RoutingAttributes creates a composite routing key from the listed attributes.
	//
	// For traces, attributes can come from resource, scope, or span, plus the pseudo attributes "span.kind" and
	// "span.name".
	// For metrics, attributes can come from resource, scope, or datapoint attributes.
	RoutingAttributes []string `mapstructure:"routing_attributes"`
}

// Validate checks if the exporter configuration is valid.
func (c *Config) Validate() error {
	// routing_attributes only has meaning when routing_key=attributes.
	if c.RoutingKey == attrRoutingStr && len(c.RoutingAttributes) == 0 {
		return fmt.Errorf("routing_attributes must be specified when routing_key is %q", attrRoutingStr)
	}

	if c.RoutingKey != attrRoutingStr && len(c.RoutingAttributes) > 0 {
		return fmt.Errorf("routing_attributes can only be used when routing_key is %q; got %q. Remove routing_attributes or set routing_key to %q", attrRoutingStr, c.RoutingKey, attrRoutingStr)
	}

	if c.Resolver.Static.HasValue() {
		static := c.Resolver.Static.Get()
		if len(static.Hostnames) > 0 && len(static.Endpoints) > 0 {
			return fmt.Errorf("resolver.static.hostnames and resolver.static.endpoints are mutually exclusive")
		}
		for _, endpoint := range static.Endpoints {
			if endpoint.Endpoint == "" {
				return fmt.Errorf("resolver.static.endpoints.endpoint must be specified")
			}
			if endpoint.Weight <= 0 {
				return fmt.Errorf("resolver.static.endpoints.weight must be greater than zero")
			}
		}
	}

	switch c.routingAlgorithm() {
	case consistentHashingRoutingAlgorithmStr,
		roundRobinRoutingAlgorithmStr,
		weightedRoundRobinRoutingAlgorithmStr,
		leastConnectionsRoutingAlgorithmStr,
		leastResponseTimeRoutingAlgorithmStr,
		powerOfTwoChoicesRoutingAlgorithmStr:
	default:
		return fmt.Errorf("unsupported routing.algorithm: %q", c.Routing.Algorithm)
	}

	if c.routingAlgorithm() == weightedRoundRobinRoutingAlgorithmStr {
		if !c.Resolver.Static.HasValue() || len(c.Resolver.Static.Get().Endpoints) == 0 {
			return fmt.Errorf("routing.algorithm %q requires resolver.static.endpoints with weights", weightedRoundRobinRoutingAlgorithmStr)
		}
	}

	return nil
}

func (c *Config) routingAlgorithm() string {
	if c.Routing.Algorithm == "" {
		return consistentHashingRoutingAlgorithmStr
	}
	return c.Routing.Algorithm
}

// Protocol holds the individual protocol-specific settings. Only OTLP is supported at the moment.
type Protocol struct {
	OTLP otlpexporter.Config `mapstructure:"otlp"`
	// prevent unkeyed literal initialization
	_ struct{}
}

// ResolverSettings defines the configurations for the backend resolver
type ResolverSettings struct {
	Static      configoptional.Optional[StaticResolver]      `mapstructure:"static"`
	DNS         configoptional.Optional[DNSResolver]         `mapstructure:"dns"`
	K8sSvc      configoptional.Optional[K8sSvcResolver]      `mapstructure:"k8s"`
	AWSCloudMap configoptional.Optional[AWSCloudMapResolver] `mapstructure:"aws_cloud_map"`
	// prevent unkeyed literal initialization
	_ struct{}
}

type RoutingSettings struct {
	Algorithm          string                                            `mapstructure:"algorithm"`
	ConsistentHashing  configoptional.Optional[struct{}]                 `mapstructure:"consistent_hashing"`
	RoundRobin         configoptional.Optional[struct{}]                 `mapstructure:"round_robin"`
	WeightedRoundRobin configoptional.Optional[struct{}]                 `mapstructure:"weighted_round_robin"`
	LeastConnections   configoptional.Optional[struct{}]                 `mapstructure:"least_connections"`
	LeastResponseTime  configoptional.Optional[LeastResponseTimeRouting] `mapstructure:"least_response_time"`
	PowerOfTwoChoices  configoptional.Optional[PowerOfTwoChoicesRouting] `mapstructure:"power_of_two_choices"`
	_                  struct{}
}

type LeastResponseTimeRouting struct {
	Latency LeastResponseTimeLatency `mapstructure:"latency"`
	_       struct{}
}

type LeastResponseTimeLatency struct {
	Aggregation string  `mapstructure:"aggregation"`
	Alpha       float64 `mapstructure:"alpha"`
	_           struct{}
}

type PowerOfTwoChoicesRouting struct {
	Score    string `mapstructure:"score"`
	Fallback string `mapstructure:"fallback"`
	_        struct{}
}

// StaticResolver defines the configuration for the resolver providing a fixed list of backends
type StaticResolver struct {
	Hostnames []string         `mapstructure:"hostnames"`
	Endpoints []StaticEndpoint `mapstructure:"endpoints"`
	// prevent unkeyed literal initialization
	_ struct{}
}

type StaticEndpoint struct {
	Endpoint string `mapstructure:"endpoint"`
	Weight   int64  `mapstructure:"weight"`
	_        struct{}
}

func (r StaticResolver) BackendEndpoints() []string {
	if len(r.Endpoints) > 0 {
		endpoints := make([]string, 0, len(r.Endpoints))
		for _, endpoint := range r.Endpoints {
			endpoints = append(endpoints, endpoint.Endpoint)
		}
		return endpoints
	}
	return r.Hostnames
}

func (r StaticResolver) EndpointWeights() map[string]int64 {
	weights := make(map[string]int64, len(r.Endpoints))
	for _, endpoint := range r.Endpoints {
		weights[endpoint.Endpoint] = endpoint.Weight
	}
	return weights
}

// DNSResolver defines the configuration for the DNS resolver
type DNSResolver struct {
	Hostname string        `mapstructure:"hostname"`
	Port     string        `mapstructure:"port"`
	Interval time.Duration `mapstructure:"interval"`
	Timeout  time.Duration `mapstructure:"timeout"`
	// prevent unkeyed literal initialization
	_ struct{}
}

// K8sSvcResolver defines the configuration for the DNS resolver
type K8sSvcResolver struct {
	Service         string        `mapstructure:"service"`
	Ports           []int32       `mapstructure:"ports"`
	Timeout         time.Duration `mapstructure:"timeout"`
	ReturnHostnames bool          `mapstructure:"return_hostnames"`
	// prevent unkeyed literal initialization
	_ struct{}
}

type AWSCloudMapResolver struct {
	NamespaceName string                   `mapstructure:"namespace"`
	ServiceName   string                   `mapstructure:"service_name"`
	HealthStatus  types.HealthStatusFilter `mapstructure:"health_status"`
	Interval      time.Duration            `mapstructure:"interval"`
	Timeout       time.Duration            `mapstructure:"timeout"`
	Port          *uint16                  `mapstructure:"port"`
}
