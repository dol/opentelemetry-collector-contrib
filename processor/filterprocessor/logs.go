// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filterprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/expr"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterconfig"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterlog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor/internal/condition"
)

type filterLogProcessor struct {
	consumers         []condition.LogsConsumer
	skipResourceExpr  expr.BoolExpr[*ottlresource.TransformContext]
	skipLogRecordExpr expr.BoolExpr[*ottllog.TransformContext]
	telemetry         *filterTelemetry
	logger            *zap.Logger
}

func newFilterLogsProcessor(set processor.Settings, cfg *Config) (*filterLogProcessor, error) {
	flp := &filterLogProcessor{
		logger: set.Logger,
	}

	fpt, err := newFilterTelemetry(set, pipeline.SignalLogs, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating filter processor telemetry: %w", err)
	}
	flp.telemetry = fpt

	if len(cfg.LogConditions) > 0 {
		pc, collectionErr := cfg.newLogParserCollection(set.TelemetrySettings)
		if collectionErr != nil {
			return nil, collectionErr
		}
		var errs error
		for _, cs := range cfg.LogConditions {
			consumer, parseErr := pc.ParseContextConditions(cs)
			errs = multierr.Append(errs, parseErr)
			flp.consumers = append(flp.consumers, consumer)
		}
		if errs != nil {
			return nil, errs
		}
		return flp, nil
	}

	if cfg.Logs.ResourceConditions != nil || cfg.Logs.LogConditions != nil {
		if cfg.Logs.ResourceConditions != nil {
			flp.skipResourceExpr, err = filterottl.NewBoolExprForResource(cfg.Logs.ResourceConditions, cfg.resourceFunctions, cfg.ErrorMode, set.TelemetrySettings)
			if err != nil {
				return nil, err
			}
		}

		if cfg.Logs.LogConditions != nil {
			flp.skipLogRecordExpr, err = filterottl.NewBoolExprForLog(cfg.Logs.LogConditions, cfg.logFunctions, cfg.ErrorMode, set.TelemetrySettings)
			if err != nil {
				return nil, err
			}
		}
		return flp, nil
	}

	cfgMatch := filterconfig.MatchConfig{}
	if cfg.Logs.Include != nil && !cfg.Logs.Include.isEmpty() {
		cfgMatch.Include = cfg.Logs.Include.matchProperties()
	}

	if cfg.Logs.Exclude != nil && !cfg.Logs.Exclude.isEmpty() {
		cfgMatch.Exclude = cfg.Logs.Exclude.matchProperties()
	}

	skipExpr, err := filterlog.NewSkipExpr(&cfgMatch)
	if err != nil {
		return nil, fmt.Errorf("failed to build skip matcher: %w", err)
	}
	flp.skipLogRecordExpr = skipExpr

	return flp, nil
}

func (flp *filterLogProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	if flp.skipResourceExpr == nil && flp.skipLogRecordExpr == nil && len(flp.consumers) == 0 {
		return ld, nil
	}

	logCountBeforeFilters := ld.LogRecordCount()
	var processedLogs plog.Logs
	var errs error
	if len(flp.consumers) > 0 {
		processedLogs, errs = flp.processConditions(ctx, ld)
	} else {
		processedLogs, errs = flp.processSkipExpression(ctx, ld)
	}

	logCountAfterFilters := processedLogs.LogRecordCount()
	flp.telemetry.record(ctx, int64(logCountBeforeFilters-logCountAfterFilters))

	if errs != nil && !errors.Is(errs, processorhelper.ErrSkipProcessingData) {
		flp.logger.Error("failed processing logs", zap.Error(errs))
		return processedLogs, errs
	}

	if processedLogs.ResourceLogs().Len() == 0 {
		return processedLogs, processorhelper.ErrSkipProcessingData
	}
	return processedLogs, nil
}

func (flp *filterLogProcessor) processConditions(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	// Snapshot per-group log-record counts before processing for group telemetry.
	type groupBefore struct {
		count   int64
		attrSet attribute.Set
	}
	var before map[string]groupBefore
	if flp.telemetry.droppedGroup != nil {
		before = make(map[string]groupBefore)
		ld.ResourceLogs().Range(func(_ int, rl plog.ResourceLogs) bool {
			key, otelAttrs := buildGroupKeyAndAttrs(rl.Resource().Attributes(), flp.telemetry.droppedGroup.attributeKeys)
			n := countScopeLogs(rl.ScopeLogs())
			if gs, ok := before[key]; ok {
				gs.count += n
				before[key] = gs
			} else {
				before[key] = groupBefore{count: n, attrSet: attribute.NewSet(otelAttrs...)}
			}
			return true
		})
	}

	var errs error
	for _, consumer := range flp.consumers {
		err := consumer.ConsumeLogs(ctx, ld)
		if err != nil {
			errs = multierr.Append(errs, err)
		}
	}

	if flp.telemetry.droppedGroup != nil && len(before) > 0 {
		after := make(map[string]int64)
		ld.ResourceLogs().Range(func(_ int, rl plog.ResourceLogs) bool {
			key, _ := buildGroupKeyAndAttrs(rl.Resource().Attributes(), flp.telemetry.droppedGroup.attributeKeys)
			after[key] += countScopeLogs(rl.ScopeLogs())
			return true
		})
		for key, gs := range before {
			if dropped := gs.count - after[key]; dropped > 0 {
				flp.telemetry.droppedGroup.recordWithSet(key, gs.attrSet, dropped)
			}
		}
	}

	return ld, errs
}

func (flp *filterLogProcessor) processSkipExpression(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	var errs error
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		if flp.skipResourceExpr != nil {
			tCtx := ottlresource.NewTransformContextPtr(rl.Resource(), rl)
			skip, err := flp.skipResourceExpr.Eval(ctx, tCtx)
			tCtx.Close()
			if err != nil {
				errs = multierr.Append(errs, err)
				return false
			}
			if skip {
				flp.telemetry.recordGroup(rl.Resource().Attributes(), countScopeLogs(rl.ScopeLogs()))
				return true
			}
		}
		if flp.skipLogRecordExpr == nil {
			return rl.ScopeLogs().Len() == 0
		}
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			lrs := sl.LogRecords()
			lrs.RemoveIf(func(lr plog.LogRecord) bool {
				tCtx := ottllog.NewTransformContextPtr(rl, sl, lr)
				skip, err := flp.skipLogRecordExpr.Eval(ctx, tCtx)
				tCtx.Close()
				if err != nil {
					errs = multierr.Append(errs, err)
					return false
				}
				if skip {
					flp.telemetry.recordGroup(rl.Resource().Attributes(), 1)
				}
				return skip
			})

			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})
	return ld, errs
}

// countScopeLogs sums the log-record count across all scope logs.
func countScopeLogs(sls plog.ScopeLogsSlice) int64 {
	n := int64(0)
	for i := 0; i < sls.Len(); i++ {
		n += int64(sls.At(i).LogRecords().Len())
	}
	return n
}
