// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package declinedservicesprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/collector/processor/processorhelper/xprocessorhelper"
	"go.opentelemetry.io/collector/processor/xprocessor"

	"go.uber.org/zap"
)

var processorCapabilities = consumer.Capabilities{MutatesData: false}

func NewFactory() processor.Factory {
	return NewFactoryWithOptions()
}

func NewFactoryWithOptions() processor.Factory {
	return xprocessor.NewFactory(
		Type,
		createDefaultConfig,
		xprocessor.WithLogs(createLogsProcessor, LogsStability),
		xprocessor.WithTraces(createTracesProcessor, TracesStability),
		xprocessor.WithMetrics(createMetricsProcessor, MetricsStability),
		xprocessor.WithProfiles(createProfilesProcessor, ProfilesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	p, err := newDeclineTracesProcessor(set, cfg.(*Config), nextConsumer)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewTraces(ctx, set, cfg, nextConsumer, p.processTraces, processorhelper.WithCapabilities(processorCapabilities))
}

func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	p, err := newDeclineLogsProcessor(set, cfg.(*Config), nextConsumer)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(ctx, set, cfg, nextConsumer, p.processLogs, processorhelper.WithCapabilities(processorCapabilities))
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	p, err := newDeclineMetricsProcessor(set, cfg.(*Config), nextConsumer)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewMetrics(ctx, set, cfg, nextConsumer, p.processMetrics, processorhelper.WithCapabilities(processorCapabilities))
}

func createProfilesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer xconsumer.Profiles,
) (xprocessor.Profiles, error) {
	p, err := newDeclineProfilesProcessor(set, cfg.(*Config), nextConsumer)
	if err != nil {
		return nil, err
	}
	return xprocessorhelper.NewProfiles(ctx, set, cfg, nextConsumer, p.processProfiles, xprocessorhelper.WithCapabilities(processorCapabilities))
}
