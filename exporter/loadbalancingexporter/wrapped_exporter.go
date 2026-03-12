// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

// wrappedExporter is an exporter that waits for the data processing to complete before shutting down.
// consumeWG has to be incremented explicitly by the consumer of the wrapped exporter.
type wrappedExporter struct {
	component.Component
	consumeWG sync.WaitGroup
	statsMu   sync.RWMutex

	endpoint string
	weight   int64

	inflightRequests   int64
	requests           int64
	latencyEWMA        float64
	latencyInitialized bool
	consecutiveFails   int64
	healthy            bool

	// we store the attributes here for both cases, to avoid new allocations on the hot path
	endpointAttr attribute.Set
	successAttr  attribute.Set
	failureAttr  attribute.Set
}

type exporterStats struct {
	inflightRequests    int64
	requests            int64
	latencyEWMA         int64
	consecutiveFailures int64
	healthy             bool
	weight              int64
}

const defaultLatencyEWMAAlpha = 0.2

func newWrappedExporter(exp component.Component, identifier string, weight int64) *wrappedExporter {
	ea := attribute.String("endpoint", identifier)
	if weight <= 0 {
		weight = 1
	}
	return &wrappedExporter{
		Component:    exp,
		endpoint:     identifier,
		weight:       weight,
		healthy:      true,
		endpointAttr: attribute.NewSet(ea),
		successAttr:  attribute.NewSet(ea, attribute.Bool("success", true)),
		failureAttr:  attribute.NewSet(ea, attribute.Bool("success", false)),
	}
}

func (we *wrappedExporter) Shutdown(ctx context.Context) error {
	we.consumeWG.Wait()
	return we.Component.Shutdown(ctx)
}

func (we *wrappedExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	te, ok := we.Component.(exporter.Traces)
	if !ok {
		return fmt.Errorf("unable to export traces, unexpected exporter type: expected exporter.Traces but got %T", we.Component)
	}
	return te.ConsumeTraces(ctx, td)
}

func (we *wrappedExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	me, ok := we.Component.(exporter.Metrics)
	if !ok {
		return fmt.Errorf("unable to export metrics, unexpected exporter type: expected exporter.Metrics but got %T", we.Component)
	}
	return me.ConsumeMetrics(ctx, md)
}

func (we *wrappedExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	le, ok := we.Component.(exporter.Logs)
	if !ok {
		return fmt.Errorf("unable to export logs, unexpected exporter type: expected exporter.Logs but got %T", we.Component)
	}
	return le.ConsumeLogs(ctx, ld)
}

func (we *wrappedExporter) beginRequest(ctx context.Context, telemetry *metadata.TelemetryBuilder) {
	we.statsMu.Lock()
	we.inflightRequests++
	we.requests++
	stats := we.snapshotLocked()
	we.statsMu.Unlock()

	telemetry.LoadbalancerBackendInflight.Record(ctx, stats.inflightRequests, metric.WithAttributeSet(we.endpointAttr))
	telemetry.LoadbalancerBackendRequests.Add(ctx, 1, metric.WithAttributeSet(we.endpointAttr))
	telemetry.LoadbalancerBackendHealthy.Record(ctx, boolToInt64(stats.healthy), metric.WithAttributeSet(we.endpointAttr))
	telemetry.LoadbalancerBackendWeight.Record(ctx, stats.weight, metric.WithAttributeSet(we.endpointAttr))
}

func (we *wrappedExporter) endRequest(ctx context.Context, telemetry *metadata.TelemetryBuilder, duration time.Duration, err error) {
	we.statsMu.Lock()
	we.inflightRequests--
	if we.inflightRequests < 0 {
		we.inflightRequests = 0
	}

	latencyMillis := duration.Milliseconds()
	if !we.latencyInitialized {
		we.latencyEWMA = float64(latencyMillis)
		we.latencyInitialized = true
	} else {
		we.latencyEWMA = defaultLatencyEWMAAlpha*float64(latencyMillis) + (1-defaultLatencyEWMAAlpha)*we.latencyEWMA
	}

	if err == nil {
		we.consecutiveFails = 0
		we.healthy = true
	} else {
		we.consecutiveFails++
		we.healthy = false
	}

	stats := we.snapshotLocked()
	we.statsMu.Unlock()

	telemetry.LoadbalancerBackendInflight.Record(ctx, stats.inflightRequests, metric.WithAttributeSet(we.endpointAttr))
	telemetry.LoadbalancerBackendLatency.Record(ctx, latencyMillis, metric.WithAttributeSet(we.endpointAttr))
	telemetry.LoadbalancerBackendLatencyEwma.Record(ctx, stats.latencyEWMA, metric.WithAttributeSet(we.endpointAttr))
	telemetry.LoadbalancerBackendConsecutiveFailures.Record(ctx, stats.consecutiveFailures, metric.WithAttributeSet(we.endpointAttr))
	telemetry.LoadbalancerBackendHealthy.Record(ctx, boolToInt64(stats.healthy), metric.WithAttributeSet(we.endpointAttr))
	telemetry.LoadbalancerBackendWeight.Record(ctx, stats.weight, metric.WithAttributeSet(we.endpointAttr))
	if err == nil {
		telemetry.LoadbalancerBackendOutcome.Add(ctx, 1, metric.WithAttributeSet(we.successAttr))
		return
	}
	telemetry.LoadbalancerBackendOutcome.Add(ctx, 1, metric.WithAttributeSet(we.failureAttr))
}

func (we *wrappedExporter) stats() exporterStats {
	we.statsMu.RLock()
	defer we.statsMu.RUnlock()
	return we.snapshotLocked()
}

func (we *wrappedExporter) snapshotLocked() exporterStats {
	return exporterStats{
		inflightRequests:    we.inflightRequests,
		requests:            we.requests,
		latencyEWMA:         int64(math.Round(we.latencyEWMA)),
		consecutiveFailures: we.consecutiveFails,
		healthy:             we.healthy,
		weight:              we.weight,
	}
}

func boolToInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
