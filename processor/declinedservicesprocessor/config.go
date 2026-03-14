package declinedservicesprocessor

import "time"

// Config holds the processor configuration.
type Config struct {
	// ResetOnCollect when true will zero counters after metrics are observed.
	ResetOnCollect bool `mapstructure:"reset_on_collect"`
	// MetricName allows configuring the metric name (optional)
	MetricName string `mapstructure:"metric_name"`
	// AggregationInterval not used for this simple prototype but reserved
	AggregationInterval time.Duration `mapstructure:"aggregation_interval"`
}

func (c *Config) Validate() error {
	return nil
}
