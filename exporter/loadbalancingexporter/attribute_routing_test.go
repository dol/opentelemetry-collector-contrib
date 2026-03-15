// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func testAttributeRoutingKey(attrs []string, values map[string]pcommon.Value) string {
	attrValues := make([]attributeRoutingValue, len(attrs))
	for i, attr := range attrs {
		if value, ok := values[attr]; ok {
			attrValues[i] = attributeRoutingValue{
				value:   value,
				present: true,
			}
		}
	}

	return buildAttributeRoutingKeyFromValues(attrs, attrValues)
}

func TestBuildAttributeRoutingKeyFromValues_DistinguishesMissingAndEmpty(t *testing.T) {
	t.Parallel()

	attrs := []string{"attr"}

	missing := buildAttributeRoutingKeyFromValues(attrs, []attributeRoutingValue{{}})
	empty := buildAttributeRoutingKeyFromValues(attrs, []attributeRoutingValue{{
		value:   pcommon.NewValueEmpty(),
		present: true,
	}})

	assert.NotEqual(t, missing, empty)
}

func TestBuildAttributeRoutingKeyFromValues_DistinguishesValueTypes(t *testing.T) {
	t.Parallel()

	attrs := []string{"attr"}

	stringKey := buildAttributeRoutingKeyFromValues(attrs, []attributeRoutingValue{{
		value:   pcommon.NewValueStr("1"),
		present: true,
	}})
	intKey := buildAttributeRoutingKeyFromValues(attrs, []attributeRoutingValue{{
		value:   pcommon.NewValueInt(1),
		present: true,
	}})

	assert.NotEqual(t, stringKey, intKey)
}
