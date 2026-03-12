// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

func (lb *loadBalancer) leastConnectionsEndpoint() string {
	endpoints := lb.availableEndpoints(true)
	if len(endpoints) == 0 {
		endpoints = lb.availableEndpoints(false)
	}
	if len(endpoints) == 0 {
		return ""
	}

	selected := endpoints[0]
	minInflight := lb.exporters[endpointWithPort(selected)].stats().inflightRequests
	for _, endpoint := range endpoints[1:] {
		inflight := lb.exporters[endpointWithPort(endpoint)].stats().inflightRequests
		if inflight < minInflight {
			selected = endpoint
			minInflight = inflight
		}
	}

	return selected
}
