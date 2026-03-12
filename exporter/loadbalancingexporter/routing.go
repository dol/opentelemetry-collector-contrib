// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import "sort"

func (lb *loadBalancer) routingEndpoint(identifier []byte) string {
	switch lb.routingAlgorithm {
	case roundRobinRoutingAlgorithmStr:
		return lb.roundRobinEndpoint()
	case weightedRoundRobinRoutingAlgorithmStr:
		return lb.weightedRoundRobinEndpoint()
	case leastConnectionsRoutingAlgorithmStr:
		return lb.leastConnectionsEndpoint()
	case leastResponseTimeRoutingAlgorithmStr:
		return lb.leastResponseTimeEndpoint()
	case powerOfTwoChoicesRoutingAlgorithmStr:
		return lb.powerOfTwoChoicesEndpoint(identifier)
	default:
		return lb.ring.endpointFor(identifier)
	}
}

func (lb *loadBalancer) availableEndpoints(healthyOnly bool) []string {
	if len(lb.resolved) == 0 {
		return nil
	}

	endpoints := make([]string, 0, len(lb.resolved))
	for _, endpoint := range lb.resolved {
		exp, found := lb.exporters[endpointWithPort(endpoint)]
		if !found {
			continue
		}
		if healthyOnly && !exp.stats().healthy {
			continue
		}
		endpoints = append(endpoints, endpoint)
	}

	sort.Strings(endpoints)
	return endpoints
}

func (lb *loadBalancer) endpointWeight(endpoint string) int64 {
	if exp, found := lb.exporters[endpointWithPort(endpoint)]; found {
		return exp.stats().weight
	}
	if weight, found := lb.endpointWeights[endpoint]; found {
		return weight
	}
	if weight, found := lb.endpointWeights[endpointWithPort(endpoint)]; found {
		return weight
	}
	return 1
}
