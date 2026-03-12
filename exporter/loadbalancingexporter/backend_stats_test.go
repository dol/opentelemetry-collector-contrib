// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestWrappedExporterRequestStats(t *testing.T) {
	shutdownCtx := context.Background() //nolint:usetesting // Context must outlive test for cleanup
	otelTelemetry := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, otelTelemetry.Shutdown(shutdownCtx))
	})
	settings := exportertest.NewNopSettings(metadata.Type)
	settings.TelemetrySettings = otelTelemetry.NewTelemetrySettings()
	telemetry, err := metadata.NewTelemetryBuilder(settings.TelemetrySettings)
	require.NoError(t, err)

	exp := newWrappedExporter(mockComponent{}, "backend-1:4317", 3)

	exp.beginRequest(t.Context(), telemetry)
	require.Equal(t, int64(1), exp.stats().inflightRequests)
	require.Equal(t, int64(1), exp.stats().requests)

	exp.endRequest(t.Context(), telemetry, 125*time.Millisecond, nil)

	stats := exp.stats()
	require.Equal(t, int64(0), stats.inflightRequests)
	require.Equal(t, int64(1), stats.requests)
	require.Equal(t, int64(0), stats.consecutiveFailures)
	require.True(t, stats.healthy)
	require.Equal(t, int64(125), stats.latencyEWMA)
	require.Equal(t, int64(3), stats.weight)

	backendRequests, err := otelTelemetry.GetMetric("otelcol_loadbalancer_backend_requests")
	require.NoError(t, err)
	sum, ok := backendRequests.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, int64(1), sum.DataPoints[0].Value)
}

func TestWrappedExporterFailureStatsAndTelemetry(t *testing.T) {
	shutdownCtx := context.Background() //nolint:usetesting // Context must outlive test for cleanup
	otelTelemetry := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, otelTelemetry.Shutdown(shutdownCtx))
	})
	settings := exportertest.NewNopSettings(metadata.Type)
	settings.TelemetrySettings = otelTelemetry.NewTelemetrySettings()
	telemetry, err := metadata.NewTelemetryBuilder(settings.TelemetrySettings)
	require.NoError(t, err)

	exp := newWrappedExporter(mockComponent{}, "backend-1:4317", 2)

	exp.beginRequest(t.Context(), telemetry)
	exp.endRequest(t.Context(), telemetry, 200*time.Millisecond, errors.New("export failed"))

	stats := exp.stats()
	require.Equal(t, int64(0), stats.inflightRequests)
	require.Equal(t, int64(1), stats.requests)
	require.Equal(t, int64(1), stats.consecutiveFailures)
	require.False(t, stats.healthy)
	require.Equal(t, int64(200), stats.latencyEWMA)
	require.Equal(t, int64(2), stats.weight)

	backendHealthy, err := otelTelemetry.GetMetric("otelcol_loadbalancer_backend_healthy")
	require.NoError(t, err)
	healthyGauge, ok := backendHealthy.Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Len(t, healthyGauge.DataPoints, 1)
	require.Equal(t, int64(0), healthyGauge.DataPoints[0].Value)
}
