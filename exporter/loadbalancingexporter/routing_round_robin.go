// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

func (lb *loadBalancer) roundRobinEndpoint() string {
	endpoints := lb.availableEndpoints(true)
	if len(endpoints) == 0 {
		endpoints = lb.availableEndpoints(false)
	}
	if len(endpoints) == 0 {
		return ""
	}

	idx := int(lb.roundRobinCount.Add(1)-1) % len(endpoints)
	return endpoints[idx]
}
