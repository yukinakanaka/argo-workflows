package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/client-go/util/workqueue"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/util/logging"
	"github.com/argoproj/argo-workflows/v4/util/telemetry"
)

func TestMetrics(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	m, te, err := CreateDefaultTestMetrics(ctx)
	require.NoError(t, err)
	// Default buckets: {5, 10, 15, 20, 25, 30}
	m.OperationCompleted(ctx, 5)
	assert.NotNil(t, te)
	attribs := attribute.NewSet()
	val, err := te.GetFloat64HistogramData(ctx, telemetry.InstrumentOperationDurationSeconds.Name(), &attribs)
	require.NoError(t, err)
	assert.Equal(t, []float64{5, 10, 15, 20, 25, 30}, val.Bounds)
	assert.Equal(t, []uint64{1, 0, 0, 0, 0, 0, 0}, val.BucketCounts)
}

func TestErrors(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	m, _, err := CreateDefaultTestMetrics(ctx)

	assert.Nil(t, m.GetCustomMetric("does-not-exist"))

	require.NoError(t, err)
	err = m.UpsertCustomMetric(ctx, &wfv1.Prometheus{
		Name: "invalid.name",
	}, "owner", func() float64 { return 0.0 })
	require.Error(t, err)

	err = m.UpsertCustomMetric(ctx, &wfv1.Prometheus{
		Name: "name",
		Labels: []*wfv1.MetricLabel{{
			Key:   "invalid-key",
			Value: "value",
		}},
	}, "owner", func() float64 { return 0.0 })
	require.Error(t, err)
}

func TestMetricGC(t *testing.T) {
	config := telemetry.MetricsConfig{
		Enabled: true,
		Path:    telemetry.DefaultPrometheusServerPath,
		Port:    telemetry.DefaultPrometheusServerPort,
		TTL:     1 * time.Second,
	}

	ctx := logging.TestContext(t.Context())
	m, _, err := createTestMetrics(ctx, &config, Callbacks{})
	require.NoError(t, err)
	const key string = `metric`

	labels := []*wfv1.MetricLabel{
		{Key: "foo", Value: "bar"},
	}
	err = m.UpsertCustomMetric(ctx, &wfv1.Prometheus{
		Name:    key,
		Labels:  labels,
		Help:    "none",
		Counter: &wfv1.Counter{Value: "0.0"},
	}, "owner", nil)
	require.NoError(t, err)
	baseCm := m.GetCustomMetric(key)
	assert.NotNil(t, baseCm)

	cm := customUserData(baseCm, true)
	assert.NotNil(t, cm)
	assert.Len(t, cm.values, 1)

	// Ensure we get at least one TTL run
	timeoutTime := time.Now().Add(time.Second * 2)
	for time.Now().Before(timeoutTime) {
		// Break if we know our test will pass.
		if len(cm.values) == 0 {
			break
		}
		// Sleep to prevent overloading test worker CPU.
		time.Sleep(100 * time.Millisecond)
	}

	assert.Empty(t, cm.values)
}

func TestRealtimeMetricGC(t *testing.T) {
	config := telemetry.MetricsConfig{
		Enabled: true,
		Path:    telemetry.DefaultPrometheusServerPath,
		Port:    telemetry.DefaultPrometheusServerPort,
		TTL:     1 * time.Second,
	}
	ctx, cancel := context.WithCancel(logging.TestContext(t.Context()))
	defer cancel()
	m, err := New(ctx, telemetry.TestScopeName, telemetry.TestScopeName, &config, Callbacks{})
	require.NoError(t, err)

	labels := []*wfv1.MetricLabel{
		{Key: "foo", Value: "bar"},
	}
	name := "realtime_metric"
	wfKey := "workflow-uid"
	err = m.UpsertCustomMetric(ctx, &wfv1.Prometheus{
		Name:   name,
		Labels: labels,
		Help:   "None",
		Gauge: &wfv1.Gauge{
			Realtime: new(true),
		}},
		wfKey,
		func() float64 { return 1.0 },
	)
	require.NoError(t, err)
	userData, ok := m.GetCustomMetric(name).GetUserdata().(*customMetricUserData)
	assert.True(t, ok)
	assert.Len(t, userData.values, 1)

	go m.customMetricsGC(ctx)

	// simulate workflow is still running.
	// ensure we get at least one TTL run
	time.Sleep(time.Second * 2)
	assert.Len(t, userData.values, 1)

	// simulate workflow is completed.
	m.CompleteRealtimeMetricsForWfUID(wfKey)
	timeoutTime := time.Now().Add(time.Second * 2)
	// Ensure we get at least one TTL run
	for time.Now().Before(timeoutTime) {
		// Break if we know our test will pass.
		if len(m.realtimeWorkflows[wfKey]) == 0 {
			break
		}
		// Sleep to prevent overloading test worker CPU.
		time.Sleep(100 * time.Millisecond)
	}
	assert.Empty(t, userData.values)
}

// ageCustomMetric pretends every value of the named custom metric was last updated age ago
func ageCustomMetric(t *testing.T, m *Metrics, name string, age time.Duration) *customMetricUserData {
	t.Helper()
	inst := m.GetCustomMetric(name)
	require.NotNil(t, inst)
	ud := customUserData(inst, true)
	ud.mutex.RLock()
	defer ud.mutex.RUnlock()
	for _, value := range ud.values {
		value.mutex.Lock()
		value.lastUpdated = time.Now().Add(-age)
		value.mutex.Unlock()
	}
	return ud
}

func upsertGauge(ctx context.Context, t *testing.T, m *Metrics, name string) {
	t.Helper()
	err := m.UpsertCustomMetric(ctx, &wfv1.Prometheus{
		Name:  name,
		Help:  "none",
		Gauge: &wfv1.Gauge{Value: "1"},
	}, "owner", nil)
	require.NoError(t, err)
}

func TestCustomMetricTTLModifier(t *testing.T) {
	// TTLs are long enough that the background GC never ticks during the test
	tests := []struct {
		name          string
		globalTTL     time.Duration
		modifierTTL   time.Duration
		age           time.Duration
		wantModified  int
		wantUnchanged int
	}{
		{name: "modifier TTL without global TTL", globalTTL: 0, modifierTTL: time.Hour, age: 2 * time.Hour, wantModified: 0, wantUnchanged: 1},
		{name: "modifier TTL longer than global TTL", globalTTL: time.Hour, modifierTTL: 3 * time.Hour, age: 2 * time.Hour, wantModified: 1, wantUnchanged: 0},
		{name: "modifier TTL shorter than global TTL", globalTTL: 3 * time.Hour, modifierTTL: time.Hour, age: 2 * time.Hour, wantModified: 0, wantUnchanged: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := telemetry.MetricsConfig{
				Enabled: true,
				TTL:     tt.globalTTL,
				Modifiers: map[string]telemetry.MetricsModifier{
					"modified_gauge": {TTL: tt.modifierTTL},
				},
			}
			ctx := logging.TestContext(t.Context())
			m, _, err := createTestMetrics(ctx, &config, Callbacks{})
			require.NoError(t, err)

			upsertGauge(ctx, t, m, "modified_gauge")
			upsertGauge(ctx, t, m, "unchanged_gauge")
			modified := ageCustomMetric(t, m, "modified_gauge", tt.age)
			unchanged := ageCustomMetric(t, m, "unchanged_gauge", tt.age)

			m.runCustomGC()

			assert.Len(t, modified.values, tt.wantModified)
			assert.Len(t, unchanged.values, tt.wantUnchanged)
		})
	}
}

func TestRealtimeCustomMetricTTLModifier(t *testing.T) {
	config := telemetry.MetricsConfig{
		Enabled: true,
		Modifiers: map[string]telemetry.MetricsModifier{
			"realtime_metric": {TTL: time.Hour},
		},
	}
	ctx := logging.TestContext(t.Context())
	m, _, err := createTestMetrics(ctx, &config, Callbacks{})
	require.NoError(t, err)

	wfKey := "workflow-uid"
	err = m.UpsertCustomMetric(ctx, &wfv1.Prometheus{
		Name:  "realtime_metric",
		Help:  "none",
		Gauge: &wfv1.Gauge{Realtime: new(true)},
	}, wfKey, func() float64 { return 1.0 })
	require.NoError(t, err)
	ud := ageCustomMetric(t, m, "realtime_metric", 2*time.Hour)

	// still running
	m.runCustomGC()
	assert.Len(t, ud.values, 1)

	m.CompleteRealtimeMetricsForWfUID(wfKey)
	m.runCustomGC()
	assert.Empty(t, ud.values)
}

func TestCustomMetricGCInterval(t *testing.T) {
	tests := []struct {
		name      string
		globalTTL time.Duration
		modifiers map[string]telemetry.MetricsModifier
		want      time.Duration
	}{
		{name: "nothing configured", want: 0},
		{name: "global only", globalTTL: time.Hour, want: time.Hour},
		{name: "modifier only", modifiers: map[string]telemetry.MetricsModifier{"a": {TTL: time.Minute}}, want: time.Minute},
		{name: "modifier without TTL", globalTTL: time.Hour, modifiers: map[string]telemetry.MetricsModifier{"a": {Disabled: true}}, want: time.Hour},
		{name: "shortest wins", globalTTL: time.Minute, modifiers: map[string]telemetry.MetricsModifier{"a": {TTL: time.Hour}, "b": {TTL: time.Second}}, want: time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := telemetry.MetricsConfig{Enabled: true, TTL: tt.globalTTL, Modifiers: tt.modifiers}
			ctx, cancel := context.WithCancel(logging.TestContext(t.Context()))
			defer cancel()
			m, _, err := createTestMetrics(ctx, &config, Callbacks{})
			require.NoError(t, err)
			assert.Equal(t, tt.want, m.gcInterval())
		})
	}
}

func TestWorkflowQueueMetrics(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	m, te, err := getSharedMetrics(ctx)
	require.NoError(t, err)
	attribs := attribute.NewSet(attribute.String(telemetry.AttribQueueName, "workflow_queue"))
	wfQueue := m.RateLimiterWithBusyWorkers(ctx, workqueue.DefaultTypedControllerRateLimiter[string](), "workflow_queue")
	defer wfQueue.ShutDown()

	assert.NotNil(t, m.GetInstrument(telemetry.InstrumentQueueDepthGauge.Name()))
	assert.NotNil(t, m.GetInstrument(telemetry.InstrumentQueueLatency.Name()))

	wfQueue.Add("hello")

	require.NotNil(t, m.GetInstrument(telemetry.InstrumentQueueAddsCount.Name()))
	val, err := te.GetInt64CounterValue(ctx, telemetry.InstrumentQueueAddsCount.Name(), &attribs)
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)
}

func TestRealTimeMetricDeletion(t *testing.T) {
	config := telemetry.MetricsConfig{
		Enabled: true,
		Path:    telemetry.DefaultPrometheusServerPath,
		Port:    telemetry.DefaultPrometheusServerPort,
		TTL:     1 * time.Second,
	}
	ctx, cancel := context.WithCancel(logging.TestContext(t.Context()))
	defer cancel()
	m, err := New(ctx, telemetry.TestScopeName, telemetry.TestScopeName, &config, Callbacks{})
	require.NoError(t, err)

	// We've not yet fed a metric in for 123
	m.DeleteRealtimeMetricsForWfUID("123")
	assert.Empty(t, m.realtimeWorkflows["123"])

	const key string = `metric`

	labels := []*wfv1.MetricLabel{
		{Key: "foo", Value: "bar"},
	}
	err = m.UpsertCustomMetric(ctx, &wfv1.Prometheus{
		Name:   key,
		Labels: labels,
		Help:   "hello",
		Gauge: &wfv1.Gauge{
			Value:     "1.0",
			Realtime:  new(true),
			Operation: wfv1.GaugeOperationAdd,
		},
	}, "123", func() float64 { return 0.0 })
	require.NoError(t, err)

	baseCm := m.GetCustomMetric(key)
	assert.NotNil(t, baseCm)

	m.DeleteRealtimeMetricsForWfUID("456")
	assert.Empty(t, m.realtimeWorkflows["456"])

	cm := customUserData(baseCm, true)
	assert.NotNil(t, cm)
	assert.Len(t, cm.values, 1)
	assert.Len(t, m.realtimeWorkflows["123"], 1)

	m.DeleteRealtimeMetricsForWfUID("123")
	assert.Empty(t, m.realtimeWorkflows["123"])
	assert.Empty(t, cm.values)

	err = m.UpsertCustomMetric(ctx, &wfv1.Prometheus{
		Name:   key,
		Labels: labels,
		Help:   "hello",
		Gauge: &wfv1.Gauge{
			Value:     "1.0",
			Realtime:  new(true),
			Operation: wfv1.GaugeOperationAdd,
		},
	}, "456", nil)
	require.NoError(t, err)

	assert.Len(t, cm.values, 1)
	assert.Len(t, m.realtimeWorkflows["456"], 1)
}
