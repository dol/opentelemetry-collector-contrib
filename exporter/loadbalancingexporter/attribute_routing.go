// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
)

type attributeRoutingValue struct {
	value   pcommon.Value
	present bool
}

var routingFormatterPool = &sync.Pool{
	New: func() any { return pdatautil.NewTypedSeparatorFormatter() },
}

func buildAttributeRoutingKey(attrs []string, lookup func(attr string) (pcommon.Value, bool)) string {
	values := make([]attributeRoutingValue, len(attrs))
	for i, attr := range attrs {
		if value, ok := lookup(attr); ok {
			values[i] = attributeRoutingValue{
				value:   value,
				present: true,
			}
		}
	}

	return buildAttributeRoutingKeyFromValues(attrs, values)
}

// buildAttributeRoutingKeyFromValues deterministically combines the configured
// attribute names, their present/missing state, and their values in config
// order into a stable byte sequence. The load balancer hashes this identifier
// later, so we avoid doing an additional digest pass here.
func buildAttributeRoutingKeyFromValues(attrs []string, values []attributeRoutingValue) string {
	f := routingFormatterPool.Get().(*pdatautil.TypedSeparatorFormatter)
	defer routingFormatterPool.Put(f)
	f.Reset()

	for i, attr := range attrs {
		f.WriteString(attr)
		if !values[i].present {
			f.WriteMissing()
			continue
		}

		f.WriteValue(values[i].value)
	}

	return f.String()
}
