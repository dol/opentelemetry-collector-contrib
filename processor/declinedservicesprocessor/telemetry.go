package declinedservicesprocessor

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// declineTelemetry handles the observable metric registration.
type declineTelemetry struct {
	meter metric.Meter
	mu    sync.Mutex
	reg   metric.Int64ObservableUpDownCounter
	regs  []metric.Registration
}

func newDeclineTelemetry(set component.TelemetrySettings) (*declineTelemetry, error) {
	meter := set.MeterProvider.Meter("github.com/open-telemetry/opentelemetry-collector-contrib/processor/declinedservicesprocessor")
	var err error
	var dt declineTelemetry
	dt.meter = meter
	dt.reg, err = meter.Int64ObservableUpDownCounter(
		"otelcol_declined_services.count",
		metric.WithDescription("Number of declined items seen per service.name [Development]"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	return &dt, nil
}

// RegisterReportFunc registers a callback that will be invoked on metrics collection.
// The reportFunc should return a map of serviceName->count to be reported. If resetOnReport is true, the underlying storage will be assumed to be reset by the reportFunc.
func (dt *declineTelemetry) RegisterReportFunc(reportFunc func() map[string]int64) error {
	if dt == nil {
		return errors.New("nil telemetry")
	}
	reg, err := dt.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		m := reportFunc()
		for svc, cnt := range m {
			if cnt <= 0 {
				continue
			}
			o.ObserveInt64(dt.reg, cnt, attribute.String("service.name", svc))
		}
		return nil
	}, dt.reg)
	if err != nil {
		return err
	}
	dt.mu.Lock()
	dt.regs = append(dt.regs, reg)
	dt.mu.Unlock()
	return nil
}

func (dt *declineTelemetry) Shutdown() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	for _, r := range dt.regs {
		r.Unregister()
	}
}
