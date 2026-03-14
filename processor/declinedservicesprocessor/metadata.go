package declinedservicesprocessor

import (
	"go.opentelemetry.io/collector/component"
)

// metadata mirrors other processors' metadata stubs used for factory.
var (
	Type              = component.NewID("declinedservices")
	LogsStability     = component.StabilityLevelDevelopment
	TracesStability   = component.StabilityLevelDevelopment
	MetricsStability  = component.StabilityLevelDevelopment
	ProfilesStability = component.StabilityLevelDevelopment
)
