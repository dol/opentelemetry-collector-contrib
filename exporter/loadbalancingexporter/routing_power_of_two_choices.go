// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

func (lb *loadBalancer) powerOfTwoChoicesEndpoint(identifier []byte) string {
	endpoints := lb.availableEndpoints(true)
	if len(endpoints) < 2 {
		return lb.ring.endpointFor(identifier)
	}

	first, second := lb.sampleTwoEndpoints(endpoints)
	return lb.pickLowerInflight(first, second)
}

func (lb *loadBalancer) sampleTwoEndpoints(endpoints []string) (string, string) {
	if len(endpoints) == 2 {
		return endpoints[0], endpoints[1]
	}

	lb.randMu.Lock()
	firstIdx := lb.random.IntN(len(endpoints))
	secondIdx := lb.random.IntN(len(endpoints) - 1)
	if secondIdx >= firstIdx {
		secondIdx++
	}
	lb.randMu.Unlock()

	return endpoints[firstIdx], endpoints[secondIdx]
}

func (lb *loadBalancer) pickLowerInflight(first string, second string) string {
	firstInflight := lb.exporters[endpointWithPort(first)].stats().inflightRequests
	secondInflight := lb.exporters[endpointWithPort(second)].stats().inflightRequests

	if firstInflight < secondInflight {
		return first
	}
	if secondInflight < firstInflight {
		return second
	}
	if first < second {
		return first
	}
	return second
}
