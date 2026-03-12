// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

func (lb *loadBalancer) leastResponseTimeEndpoint() string {
	endpoints := lb.availableEndpoints(true)
	if len(endpoints) == 0 {
		endpoints = lb.availableEndpoints(false)
	}
	if len(endpoints) == 0 {
		return ""
	}

	var (
		selected   string
		minLatency int64
		found      bool
	)
	for _, endpoint := range endpoints {
		exp := lb.exporters[endpointWithPort(endpoint)]
		exp.statsMu.RLock()
		initialized := exp.latencyInitialized
		latency := int64(exp.latencyEWMA)
		exp.statsMu.RUnlock()
		if !initialized {
			continue
		}
		if !found || latency < minLatency {
			selected = endpoint
			minLatency = latency
			found = true
		}
	}
	if found {
		return selected
	}

	return endpoints[0]
}
