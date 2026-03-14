// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filterprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/expr"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspanevent"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor/internal/condition"
)

type filterSpanProcessor struct {
	consumers         []condition.TracesConsumer
	skipResourceExpr  expr.BoolExpr[*ottlresource.TransformContext]
	skipSpanExpr      expr.BoolExpr[*ottlspan.TransformContext]
	skipSpanEventExpr expr.BoolExpr[*ottlspanevent.TransformContext]
	telemetry         *filterTelemetry
	logger            *zap.Logger
}

func newFilterSpansProcessor(set processor.Settings, cfg *Config) (*filterSpanProcessor, error) {
	var err error
	fsp := &filterSpanProcessor{
		logger: set.Logger,
	}

	fpt, err := newFilterTelemetry(set, pipeline.SignalTraces, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating filter processor telemetry: %w", err)
	}
	fsp.telemetry = fpt

	if len(cfg.TraceConditions) > 0 {
		pc, collectionErr := cfg.newTraceParserCollection(set.TelemetrySettings)
		if collectionErr != nil {
			return nil, collectionErr
		}
		var errs error
		for _, cs := range cfg.TraceConditions {
			consumer, parseErr := pc.ParseContextConditions(cs)
			errs = multierr.Append(errs, parseErr)
			fsp.consumers = append(fsp.consumers, consumer)
		}
		if errs != nil {
			return nil, errs
		}
		return fsp, nil
	}

	if cfg.Traces.ResourceConditions != nil || cfg.Traces.SpanConditions != nil || cfg.Traces.SpanEventConditions != nil {
		if cfg.Traces.ResourceConditions != nil {
			fsp.skipResourceExpr, err = filterottl.NewBoolExprForResource(cfg.Traces.ResourceConditions, cfg.resourceFunctions, cfg.ErrorMode, set.TelemetrySettings)
			if err != nil {
				return nil, err
			}
		}

		if cfg.Traces.SpanConditions != nil {
			fsp.skipSpanExpr, err = filterottl.NewBoolExprForSpan(cfg.Traces.SpanConditions, cfg.spanFunctions, cfg.ErrorMode, set.TelemetrySettings)
			if err != nil {
				return nil, err
			}
		}
		if cfg.Traces.SpanEventConditions != nil {
			fsp.skipSpanEventExpr, err = filterottl.NewBoolExprForSpanEvent(cfg.Traces.SpanEventConditions, cfg.spanEventFunctions, cfg.ErrorMode, set.TelemetrySettings)
			if err != nil {
				return nil, err
			}
		}
		return fsp, nil
	}

	fsp.skipSpanExpr, err = filterspan.NewSkipExpr(&cfg.Spans)
	if err != nil {
		return nil, err
	}

	includeMatchType, excludeMatchType := "[None]", "[None]"
	if cfg.Spans.Include != nil {
		includeMatchType = string(cfg.Spans.Include.MatchType)
	}

	if cfg.Spans.Exclude != nil {
		excludeMatchType = string(cfg.Spans.Exclude.MatchType)
	}

	set.Logger.Info(
		"Span filter configured",
		zap.String("[Include] match_type", includeMatchType),
		zap.String("[Exclude] match_type", excludeMatchType),
	)

	return fsp, nil
}

// processTraces filters the given spans of a traces based off the filterSpanProcessor's filters.
func (fsp *filterSpanProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if fsp.skipResourceExpr == nil && fsp.skipSpanExpr == nil && fsp.skipSpanEventExpr == nil && len(fsp.consumers) == 0 {
		return td, nil
	}

	spanCountBeforeFilters := td.SpanCount()

	var errs error
	var processedTraces ptrace.Traces
	if len(fsp.consumers) > 0 {
		processedTraces, errs = fsp.processConditions(ctx, td)
	} else {
		processedTraces, errs = fsp.processSkipExpression(ctx, td)
	}

	spanCountAfterFilters := td.SpanCount()
	fsp.telemetry.record(ctx, int64(spanCountBeforeFilters-spanCountAfterFilters))

	if errs != nil && !errors.Is(errs, processorhelper.ErrSkipProcessingData) {
		fsp.logger.Error("failed processing traces", zap.Error(errs))
		return processedTraces, errs
	}

	if processedTraces.ResourceSpans().Len() == 0 {
		return processedTraces, processorhelper.ErrSkipProcessingData
	}
	return processedTraces, nil
}

func (fsp *filterSpanProcessor) processSkipExpression(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	var errs error
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		resource := rs.Resource()
		if fsp.skipResourceExpr != nil {
			tCtx := ottlresource.NewTransformContextPtr(resource, rs)
			skip, err := fsp.skipResourceExpr.Eval(ctx, tCtx)
			tCtx.Close()
			if err != nil {
				errs = multierr.Append(errs, err)
				return false
			}
			if skip {
				fsp.telemetry.recordGroup(resource.Attributes(), countScopeSpans(rs.ScopeSpans()))
				return true
			}
		}
		if fsp.skipSpanExpr == nil && fsp.skipSpanEventExpr == nil {
			return rs.ScopeSpans().Len() == 0
		}
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				if fsp.skipSpanExpr != nil {
					tCtx := ottlspan.NewTransformContextPtr(rs, ss, span)
					skip, err := fsp.skipSpanExpr.Eval(ctx, tCtx)
					tCtx.Close()
					if err != nil {
						errs = multierr.Append(errs, err)
						return false
					}
					if skip {
						fsp.telemetry.recordGroup(resource.Attributes(), 1)
						return true
					}
				}
				if fsp.skipSpanEventExpr != nil {
					span.Events().RemoveIf(func(spanEvent ptrace.SpanEvent) bool {
						tCtx := ottlspanevent.NewTransformContextPtr(rs, ss, span, spanEvent)
						skip, err := fsp.skipSpanEventExpr.Eval(ctx, tCtx)
						tCtx.Close()
						if err != nil {
							errs = multierr.Append(errs, err)
							return false
						}
						return skip
					})
				}
				return false
			})
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})
	return td, errs
}

// countScopeSpans sums the number of spans across all scope spans slices.
func countScopeSpans(sss ptrace.ScopeSpansSlice) int64 {
	n := int64(0)
	for i := 0; i < sss.Len(); i++ {
		n += int64(sss.At(i).Spans().Len())
	}
	return n
}

func (fsp *filterSpanProcessor) processConditions(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	// Snapshot per-group span counts before processing so we can report what was dropped.
	type groupBefore struct {
		count   int64
		attrSet attribute.Set
	}
	var before map[string]groupBefore
	if fsp.telemetry.droppedGroup != nil {
		before = make(map[string]groupBefore)
		td.ResourceSpans().Range(func(_ int, rs ptrace.ResourceSpans) bool {
			key, otelAttrs := buildGroupKeyAndAttrs(rs.Resource().Attributes(), fsp.telemetry.droppedGroup.attributeKeys)
			n := countScopeSpans(rs.ScopeSpans())
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
	for _, consumer := range fsp.consumers {
		err := consumer.ConsumeTraces(ctx, td)
		if err != nil {
			errs = multierr.Append(errs, err)
		}
	}

	// Record per-group drops by comparing counts after processing.
	if fsp.telemetry.droppedGroup != nil && len(before) > 0 {
		after := make(map[string]int64)
		td.ResourceSpans().Range(func(_ int, rs ptrace.ResourceSpans) bool {
			key, _ := buildGroupKeyAndAttrs(rs.Resource().Attributes(), fsp.telemetry.droppedGroup.attributeKeys)
			after[key] += countScopeSpans(rs.ScopeSpans())
			return true
		})
		for key, gs := range before {
			if dropped := gs.count - after[key]; dropped > 0 {
				fsp.telemetry.droppedGroup.recordWithSet(key, gs.attrSet, dropped)
			}
		}
	}

	return td, errs
}
