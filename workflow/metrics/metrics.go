package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/argoproj/argo-workflows/v4/util/logging"
	"github.com/argoproj/argo-workflows/v4/util/telemetry"

	metricsdk "go.opentelemetry.io/otel/sdk/metric"
)

type Metrics struct {
	*telemetry.Metrics

	callbacks          Callbacks
	realtimeMutex      sync.Mutex
	customMetricsMutex sync.Mutex
	realtimeWorkflows  map[string][]realtimeTracker
	customTTL          time.Duration
	customTTLOverrides map[string]time.Duration
	fallbackLogger     logging.Logger // use a logger from context if available
}

func New(ctx context.Context, serviceName, prometheusName string, config *telemetry.MetricsConfig, callbacks Callbacks, extraOpts ...metricsdk.Option) (*Metrics, error) {
	m, err := telemetry.NewMetrics(ctx, serviceName, prometheusName, config, extraOpts...)
	if err != nil {
		return nil, err
	}

	err = m.Populate(ctx,
		telemetry.AddVersion,
		telemetry.AddDeprecationCounter,
	)
	if err != nil {
		return nil, err
	}

	metrics := &Metrics{
		Metrics:            m,
		callbacks:          callbacks,
		realtimeWorkflows:  make(map[string][]realtimeTracker),
		customTTL:          config.TTL,
		customTTLOverrides: make(map[string]time.Duration),
		fallbackLogger:     logging.RequireLoggerFromContext(ctx),
	}

	for name, modifier := range config.Modifiers {
		if modifier.TTL > 0 {
			metrics.customTTLOverrides[name] = modifier.TTL
		}
	}

	err = metrics.populate(ctx,
		addIsLeader,
		addPodPhaseGauge,
		addPodPhaseCounter,
		addPodMissingCounter,
		addPodPendingCounter,
		addPodRestartCounter,
		addWorkflowPhaseGauge,
		addCronWfTriggerCounter,
		addCronWfPolicyCounter,
		addLocksTakenCounter,
		addLocksGauges,
		addWorkflowPhaseCounter,
		addWorkflowTemplateCounter,
		addWorkflowTemplateHistogram,
		addOperationDurationHistogram,
		addErrorCounter,
		addLogCounter,
		addK8sRequests,
		addClientRateLimiterLatency,
		addWorkflowConditionGauge,
		addWorkQueueMetrics,
		addResourceRateLimiterLatency,
	)
	if err != nil {
		return nil, err
	}

	go metrics.customMetricsGC(ctx)

	return metrics, nil
}

type addMetric func(context.Context, *Metrics) error

func (m *Metrics) populate(ctx context.Context, adders ...addMetric) error {
	for _, adder := range adders {
		if err := adder(ctx, m); err != nil {
			return err
		}
	}
	return nil
}
