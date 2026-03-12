// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

func (lb *loadBalancer) weightedRoundRobinEndpoint() string {
	endpoints := lb.availableEndpoints(true)
	if len(endpoints) == 0 {
		endpoints = lb.availableEndpoints(false)
	}
	if len(endpoints) == 0 {
		return ""
	}

	totalWeight := 0
	weights := make([]int, len(endpoints))
	for i, endpoint := range endpoints {
		weight := int(lb.endpointWeight(endpoint))
		if weight <= 0 {
			weight = 1
		}
		weights[i] = weight
		totalWeight += weight
	}

	idx := int(lb.roundRobinCount.Add(1)-1) % totalWeight
	for i, endpoint := range endpoints {
		if idx < weights[i] {
			return endpoint
		}
		idx -= weights[i]
	}

	return endpoints[0]
}
