package declinedservicesprocessor

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// processor keeps counters per service name and exposes them via an observable metric.
type declineProcessor struct {
	logger    *zap.Logger
	cfg       *Config
	telemetry *declineTelemetry
	// counts stores the aggregated counts since last report. Protected by mu.
	counts map[string]int64
	mu     sync.Mutex
	// next consumers to forward data
	nextTraces   consumer.Traces
	nextLogs     consumer.Logs
	nextMetrics  consumer.Metrics
	nextProfiles xconsumer.Profiles
}

func newDeclineTracesProcessor(set processor.Settings, cfg *Config, next consumer.Traces) (*declineProcessor, error) {
	dt, err := newDeclineTelemetry(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	p := &declineProcessor{
		logger:     set.Logger,
		cfg:        cfg,
		telemetry:  dt,
		counts:     map[string]int64{},
		nextTraces: next,
	}
	// register report func that will be called on metric collection. It returns a snapshot and resets counts if configured.
	err = dt.RegisterReportFunc(func() map[string]int64 {
		p.mu.Lock()
		m := p.counts
		p.counts = map[string]int64{}
		p.mu.Unlock()
		return m
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

func newDeclineLogsProcessor(set processor.Settings, cfg *Config, next consumer.Logs) (*declineProcessor, error) {
	dt, err := newDeclineTelemetry(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	p := &declineProcessor{
		logger:    set.Logger,
		cfg:       cfg,
		telemetry: dt,
		counts:    map[string]int64{},
		nextLogs:  next,
	}
	err = dt.RegisterReportFunc(func() map[string]int64 {
		p.mu.Lock()
		m := p.counts
		p.counts = map[string]int64{}
		p.mu.Unlock()
		return m
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

func newDeclineMetricsProcessor(set processor.Settings, cfg *Config, next consumer.Metrics) (*declineProcessor, error) {
	dt, err := newDeclineTelemetry(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	p := &declineProcessor{
		logger:      set.Logger,
		cfg:         cfg,
		telemetry:   dt,
		counts:      map[string]int64{},
		nextMetrics: next,
	}
	err = dt.RegisterReportFunc(func() map[string]int64 {
		p.mu.Lock()
		m := p.counts
		p.counts = map[string]int64{}
		p.mu.Unlock()
		return m
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

func newDeclineProfilesProcessor(set processor.Settings, cfg *Config, next xconsumer.Profiles) (*declineProcessor, error) {
	dt, err := newDeclineTelemetry(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	p := &declineProcessor{
		logger:       set.Logger,
		cfg:          cfg,
		telemetry:    dt,
		counts:       map[string]int64{},
		nextProfiles: next,
	}
	err = dt.RegisterReportFunc(func() map[string]int64 {
		p.mu.Lock()
		m := p.counts
		p.counts = map[string]int64{}
		p.mu.Unlock()
		return m
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

// helper to extract service.name from resource
func serviceNameFromResource(r pcommon.Resource) (string, bool) {
	if r.IsNil() {
		return "", false
	}
	v, ok := r.Attributes().Get("service.name")
	if !ok {
		return "", false
	}
	if v.Type() == pcommon.ValueTypeString {
		return v.StringVal(), true
	}
	return "", false
}

func (p *declineProcessor) addCount(service string, n int64) {
	if service == "" {
		service = "<unknown>"
	}
	p.mu.Lock()
	p.counts[service] += n
	p.mu.Unlock()
}

func (p *declineProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if td.SpanCount() == 0 {
		return td, nil
	}
	// count spans per resource/service
	td.ResourceSpans().Range(func(i int, rs ptrace.ResourceSpans) bool {
		svc, ok := serviceNameFromResource(rs.Resource())
		if !ok {
			svc = ""
		}
		count := int64(rs.ScopeSpans().Len())
		// more accurate is td.SpanCount per-resource but this is a simple estimate
		p.addCount(svc, count)
		return true
	})
	return td, nil
}

func (p *declineProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	if ld.LogRecordCount() == 0 {
		return ld, nil
	}
	ld.ResourceLogs().Range(func(i int, rl plog.ResourceLogs) bool {
		svc, ok := serviceNameFromResource(rl.Resource())
		if !ok {
			svc = ""
		}
		count := int64(rl.ScopeLogs().Len())
		p.addCount(svc, count)
		return true
	})
	return ld, nil
}

func (p *declineProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.DataPointCount() == 0 {
		return md, nil
	}
	md.ResourceMetrics().Range(func(i int, rm pmetric.ResourceMetrics) bool {
		svc, ok := serviceNameFromResource(rm.Resource())
		if !ok {
			svc = ""
		}
		count := int64(rm.ScopeMetrics().Len())
		p.addCount(svc, count)
		return true
	})
	return md, nil
}

func (p *declineProcessor) processProfiles(ctx context.Context, xp xconsumer.Profiles) (xconsumer.Profiles, error) {
	// Profiles support is left as a no-op counting 1 per call (prototype)
	p.addCount("", 1)
	return xp, nil
}
