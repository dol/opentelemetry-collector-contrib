// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filterprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/pipeline/xpipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor/internal/metadata"
)

const (
	defaultDroppedGroupMetricName = "otelcol_filter_dropped_by_group"
	defaultMaxGroupEntries        = 1000
	overflowGroupKey              = "_overflow"
)

// droppedGroupEntry holds an accumulated count and the corresponding OTel attribute set.
type droppedGroupEntry struct {
	count   int64
	attrSet attribute.Set
}

// droppedGroupTelemetry counts dropped items per distinct resource-attribute-value group and
// exposes them as a single observable OTLP metric.  Counters optionally reset after each
// collection (reset_on_collect: true) to implement "release on scrape" semantics.
type droppedGroupTelemetry struct {
	mu             sync.Mutex
	entries        map[string]*droppedGroupEntry
	attributeKeys  []string
	maxEntries     int
	resetOnCollect bool
	observable     metric.Int64ObservableUpDownCounter
	registration   metric.Registration
}

func newDroppedGroupTelemetry(set processor.Settings, cfg DroppedGroupByConfig) (*droppedGroupTelemetry, error) {
	metricName := cfg.MetricName
	if metricName == "" {
		metricName = defaultDroppedGroupMetricName
	}

	maxEntries := cfg.MaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultMaxGroupEntries
	}

	m := metadata.Meter(set.TelemetrySettings)
	obs, err := m.Int64ObservableUpDownCounter(
		metricName,
		metric.WithDescription("Number of items dropped by the filter processor grouped by resource attributes [Development]"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s observable metric: %w", metricName, err)
	}

	dgt := &droppedGroupTelemetry{
		entries:        map[string]*droppedGroupEntry{},
		attributeKeys:  cfg.Attributes,
		maxEntries:     maxEntries,
		resetOnCollect: cfg.ResetOnCollect,
		observable:     obs,
	}

	reg, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		dgt.mu.Lock()
		snapshot := dgt.entries
		if dgt.resetOnCollect {
			dgt.entries = map[string]*droppedGroupEntry{}
		}
		dgt.mu.Unlock()
		for _, e := range snapshot {
			if e.count > 0 {
				o.ObserveInt64(obs, e.count, metric.WithAttributeSet(e.attrSet))
			}
		}
		return nil
	}, obs)
	if err != nil {
		return nil, fmt.Errorf("failed to register %s callback: %w", metricName, err)
	}
	dgt.registration = reg
	return dgt, nil
}

// record extracts the configured attribute keys from attrs, builds a map key, and increments
// the counter for that group by n.  When the map is full, n spills into the overflow bucket.
func (dgt *droppedGroupTelemetry) record(attrs pcommon.Map, n int64) {
	if n <= 0 {
		return
	}
	mapKey, otelAttrs := buildGroupKeyAndAttrs(attrs, dgt.attributeKeys)
	dgt.mu.Lock()
	defer dgt.mu.Unlock()
	if e, ok := dgt.entries[mapKey]; ok {
		e.count += n
		return
	}
	if len(dgt.entries) >= dgt.maxEntries {
		dgt.recordOverflowLocked(n)
		return
	}
	dgt.entries[mapKey] = &droppedGroupEntry{count: n, attrSet: attribute.NewSet(otelAttrs...)}
}

// recordWithSet uses a pre-built map key and attribute.Set; used by the before/after snapshot
// path in processConditions where attrs are captured before items are removed.
func (dgt *droppedGroupTelemetry) recordWithSet(mapKey string, attrSet attribute.Set, n int64) {
	if n <= 0 {
		return
	}
	dgt.mu.Lock()
	defer dgt.mu.Unlock()
	if e, ok := dgt.entries[mapKey]; ok {
		e.count += n
		return
	}
	if len(dgt.entries) >= dgt.maxEntries {
		dgt.recordOverflowLocked(n)
		return
	}
	dgt.entries[mapKey] = &droppedGroupEntry{count: n, attrSet: attrSet}
}

func (dgt *droppedGroupTelemetry) recordOverflowLocked(n int64) {
	if e, ok := dgt.entries[overflowGroupKey]; ok {
		e.count += n
	} else {
		dgt.entries[overflowGroupKey] = &droppedGroupEntry{
			count:   n,
			attrSet: attribute.NewSet(attribute.String("_overflow", "true")),
		}
	}
}

func (dgt *droppedGroupTelemetry) shutdown() {
	if dgt.registration != nil {
		dgt.registration.Unregister()
	}
}

// buildGroupKeyAndAttrs extracts the configured attributeKeys from attrs and returns a stable
// null-byte-joined map key and the corresponding OTel KeyValue slice.
func buildGroupKeyAndAttrs(attrs pcommon.Map, attributeKeys []string) (string, []attribute.KeyValue) {
	parts := make([]string, len(attributeKeys))
	otelAttrs := make([]attribute.KeyValue, len(attributeKeys))
	for i, key := range attributeKeys {
		var val string
		if v, ok := attrs.Get(key); ok {
			val = v.AsString()
		}
		parts[i] = val
		otelAttrs[i] = attribute.String(key, val)
	}
	return strings.Join(parts, "\x00"), otelAttrs
}

// filterTelemetry bundles the per-signal global drop counter with the optional per-group counter.
type filterTelemetry struct {
	attr         metric.MeasurementOption
	counter      metric.Int64Counter
	droppedGroup *droppedGroupTelemetry // nil when dropped_group_by.attributes is empty
}

func newFilterTelemetry(set processor.Settings, signal pipeline.Signal, cfg *Config) (*filterTelemetry, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	var counter metric.Int64Counter
	switch signal {
	case pipeline.SignalMetrics:
		counter = telemetryBuilder.ProcessorFilterDatapointsFiltered
	case pipeline.SignalLogs:
		counter = telemetryBuilder.ProcessorFilterLogsFiltered
	case pipeline.SignalTraces:
		counter = telemetryBuilder.ProcessorFilterSpansFiltered
	case xpipeline.SignalProfiles:
		counter = telemetryBuilder.ProcessorFilterProfilesFiltered
	default:
		return nil, fmt.Errorf("unsupported signal type: %v", signal)
	}

	ft := &filterTelemetry{
		attr:    metric.WithAttributeSet(attribute.NewSet(attribute.String(metadata.Type.String(), set.ID.String()))),
		counter: counter,
	}

	if len(cfg.DroppedGroupBy.Attributes) > 0 {
		dgt, dgtErr := newDroppedGroupTelemetry(set, cfg.DroppedGroupBy)
		if dgtErr != nil {
			return nil, dgtErr
		}
		ft.droppedGroup = dgt
	}

	return ft, nil
}

func (fpt *filterTelemetry) record(ctx context.Context, dropped int64) {
	fpt.counter.Add(ctx, dropped, fpt.attr)
}

// recordGroup forwards n dropped items to the per-group counter (no-op when unconfigured).
func (fpt *filterTelemetry) recordGroup(attrs pcommon.Map, n int64) {
	if fpt.droppedGroup != nil {
		fpt.droppedGroup.record(attrs, n)
	}
}

func (fpt *filterTelemetry) shutdown() {
	if fpt.droppedGroup != nil {
		fpt.droppedGroup.shutdown()
	}
}
