// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	conventions "go.opentelemetry.io/otel/semconv/v1.38.0"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const errorPermanentKey = "error.permanent"

type backendSignalTelemetry struct {
	sent   metric.Int64Counter
	failed metric.Int64Counter
}

func recordBackendOutcome(ctx context.Context, telemetry backendSignalTelemetry, endpoint string, count int64, err error) {
	endpoint = endpointWithPort(endpoint)

	if err == nil {
		telemetry.sent.Add(ctx, count, metric.WithAttributeSet(attribute.NewSet(
			attribute.String("endpoint", endpoint),
		)))
		return
	}

	telemetry.failed.Add(ctx, count, metric.WithAttributeSet(attribute.NewSet(
		append([]attribute.KeyValue{attribute.String("endpoint", endpoint)}, extractFailureAttributes(err)...)...,
	)))
}

func extractFailureAttributes(err error) []attribute.KeyValue {
	if err == nil {
		return nil
	}

	return []attribute.KeyValue{
		attribute.String(string(conventions.ErrorTypeKey), determineErrorType(err)),
		attribute.Bool(errorPermanentKey, consumererror.IsPermanent(err)),
	}
}

func determineErrorType(err error) string {
	if errors.Is(err, context.Canceled) {
		return "Canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "Deadline_Exceeded"
	}

	if st, ok := status.FromError(err); ok && st.Code() != codes.OK {
		return st.Code().String()
	}

	return "_OTHER"
}
