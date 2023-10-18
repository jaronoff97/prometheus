// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	grpcExporter "go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type OTLPClient struct {
	remoteName string // Used to differentiate clients in metrics.
	urlString  string // url.String()
	Client     exporter.Metrics
	timeout    time.Duration

	mtx            sync.Mutex
	metadataMap    map[string]prompb.MetricMetadata
	startTimestamp map[uint64]int64
	lastValue      map[uint64]int64

	retryOnRateLimit bool

	readQueries         prometheus.Gauge
	readQueriesTotal    *prometheus.CounterVec
	readQueriesDuration prometheus.Observer
}

func (o *OTLPClient) StoreSamples(ctx context.Context, series []prompb.TimeSeries, i int) error {
	metrics, err := o.parseTimeSeriesBatch(series)
	if err != nil {
		return err
	}

	return o.Client.ConsumeMetrics(ctx, metrics)
}

func (o *OTLPClient) StoreMetadata(_ context.Context, metadata []prompb.MetricMetadata, _ int) error {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	for _, md := range metadata {
		o.metadataMap[md.MetricFamilyName] = md
	}

	return nil
}

func (o *OTLPClient) MetadataForMetric(name string) (prompb.MetricMetadata, bool) {
	o.mtx.Lock()
	defer o.mtx.Unlock()

	m, ok := o.metadataMap[name]
	return m, ok
}

func (o *OTLPClient) Name() string {
	return o.remoteName
}

func (o *OTLPClient) Endpoint() string {
	return o.urlString
}

// nopHost mocks a receiver.ReceiverHost for test purposes.
type nopHost struct{}

func (nh *nopHost) ReportFatalError(_ error) {}

func (nh *nopHost) GetFactory(_ component.Kind, _ component.Type) component.Factory {
	return nil
}

func (nh *nopHost) GetExtensions() map[component.ID]component.Component {
	return nil
}

func (nh *nopHost) GetExporters() map[component.DataType]map[component.ID]component.Component {
	return nil
}

// NewOTLPWriteClient creates a new client for remote write via OTLP.
func NewOTLPWriteClient(name string, conf *ClientConfig, logger log.Logger) (WriteClient, error) {
	createSettings, exporterSettings := createSettingsFromClientConfig(name, conf)
	metricsExporter, err := grpcExporter.NewFactory().CreateMetricsExporter(context.Background(), createSettings, exporterSettings)
	if err != nil {
		return nil, err
	}

	level.Info(logger).Log("msg", "Starting OTLP writer...")

	err = metricsExporter.Start(context.TODO(), &nopHost{})
	if err != nil {
		return nil, err
	}

	level.Info(logger).Log("msg", "OTLP writer started.")

	return &OTLPClient{
		remoteName:       name,
		urlString:        conf.URL.String(),
		Client:           metricsExporter,
		retryOnRateLimit: conf.RetryOnRateLimit,
		timeout:          time.Duration(conf.Timeout),
		metadataMap:      make(map[string]prompb.MetricMetadata),
		startTimestamp:   make(map[uint64]int64),
	}, nil
}

func createSettingsFromClientConfig(name string, conf *ClientConfig) (exporter.CreateSettings, *grpcExporter.Config) {
	createSettings := exporter.CreateSettings{
		ID: component.NewID("otlp/prometheuswriter"),
		TelemetrySettings: component.TelemetrySettings{
			// TODO: set otel tracer here
			Logger:         zap.Must(zap.NewDevelopment()),
			TracerProvider: trace.NewNoopTracerProvider(),
			MeterProvider:  noop.NewMeterProvider(),
			MetricsLevel:   configtelemetry.LevelNone,
			Resource:       pcommon.NewResource(),
			ReportComponentStatus: func(*component.StatusEvent) error {
				return nil
			},
		},
		BuildInfo: component.BuildInfo{
			Command:     fmt.Sprintf("prometheus ;) %s", name),
			Description: "remote write OTLP exporter",
			Version:     "TBD",
		},
	}
	headers := map[string]configopaque.String{}
	for key, val := range conf.Headers {
		headers[key] = configopaque.String(val)
	}
	exporterSettings := &grpcExporter.Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutSettings(),
		QueueSettings: exporterhelper.QueueSettings{
			Enabled:      true,
			NumConsumers: 10,
			// By default, batches are 8192 spans, for a total of up to 8 million spans in the queue
			// This can be estimated at 1-4 GB worth of maximum memory usage
			// This default is probably still too high, and may be adjusted further down in a future release
			QueueSize: 20000,
		},
		RetrySettings: exporterhelper.NewDefaultRetrySettings(),
		GRPCClientSettings: configgrpc.GRPCClientSettings{
			Endpoint: conf.URL.String(),
			TLSSetting: configtls.TLSClientSetting{
				Insecure: conf.URL.Scheme == "http",
			},
			Headers: headers,
		},
	}
	return createSettings, exporterSettings
}

func (o *OTLPClient) parseTimeSeriesBatch(series []prompb.TimeSeries) (pmetric.Metrics, error) {
	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()

	for _, ts := range series {
		metricName, attrs, labelsHash := getMetricNameAndAttributes(ts)

		metricMetadata, ok := o.MetadataForMetric(metricName)
		if !ok {
			//  we couldn't find metadata for this metric, we can't determine the metric type
			continue
		}

		// Only supports gauges, counters and histograms for now.
		if metricMetadata.Type != prompb.MetricMetadata_GAUGE && metricMetadata.Type != prompb.MetricMetadata_COUNTER && metricMetadata.Type != prompb.MetricMetadata_HISTOGRAM {
			continue
		}

		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName(metricName)
		o.parseTimeSeries(ts, metric, attrs, metricMetadata, labelsHash)
	}

	return metrics, nil
}

func (o *OTLPClient) parseTimeSeries(ts prompb.TimeSeries, metric pmetric.Metric, attrs pcommon.Map, metricMetadata prompb.MetricMetadata, hash uint64) {
	switch metricMetadata.Type {
	case prompb.MetricMetadata_GAUGE:
		gauge := metric.SetEmptyGauge()
		gaugePoints := gauge.DataPoints()

		for _, sample := range ts.Samples {
			point := gaugePoints.AppendEmpty()
			attrs.CopyTo(point.Attributes())

			point.SetTimestamp(toTimeStamp(sample.GetTimestamp()))
			point.SetStartTimestamp(o.getTimestamps(hash, sample.GetTimestamp()))
			point.SetDoubleValue(sample.GetValue())
		}

	case prompb.MetricMetadata_COUNTER:
		sum := metric.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		sumPoints := sum.DataPoints()

		for _, sample := range ts.Samples {
			point := sumPoints.AppendEmpty()
			attrs.CopyTo(point.Attributes())

			point.SetTimestamp(toTimeStamp(sample.GetTimestamp()))
			point.SetStartTimestamp(o.getTimestamps(hash, sample.GetTimestamp()))
			point.SetDoubleValue(sample.GetValue())
		}

	case prompb.MetricMetadata_HISTOGRAM:
		histo := metric.SetEmptyExponentialHistogram()
		histo.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		for _, histogram := range ts.Histograms {
			histoPoint := histo.DataPoints().AppendEmpty()

			attrs.CopyTo(histoPoint.Attributes())

			histoPoint.SetTimestamp(toTimeStamp(histogram.GetTimestamp()))
			histoPoint.SetStartTimestamp(o.getTimestamps(hash, histogram.GetTimestamp()))

			histoPoint.SetScale(histogram.Schema)
			histoPoint.SetCount(histogram.GetCountInt())
			histoPoint.SetSum(histogram.GetSum())
			histoPoint.SetZeroCount(histogram.GetZeroCountInt())

			convertBucketsLayout(histoPoint.Positive(), histogram.PositiveSpans, histogram.PositiveDeltas)
			convertBucketsLayout(histoPoint.Negative(), histogram.NegativeSpans, histogram.NegativeDeltas)

		}
	}
}

func convertBucketsLayout(buckets pmetric.ExponentialHistogramDataPointBuckets, spans []prompb.BucketSpan, deltas []int64) {
	if len(spans) == 0 {
		return
	}

	buckets.SetOffset(spans[0].Offset - 1)

	var total uint64
	for _, delta := range deltas {
		total += uint64(delta)
		buckets.BucketCounts().Append(total)
	}
}

func (o *OTLPClient) getTimestamps(hash uint64, pointMillis int64) pcommon.Timestamp {
	o.mtx.Lock()
	defer o.mtx.Unlock()

	if o.startTimestamp[hash] == 0 {
		o.startTimestamp[hash] = pointMillis - 1
	}

	return toTimeStamp(o.startTimestamp[hash])
}

func getMetricNameAndAttributes(ts prompb.TimeSeries) (string, pcommon.Map, uint64) {
	var metricName string
	attrs := pcommon.NewMap()

	// construct a labels.Label in order to get a hash for each label set.
	b := labels.ScratchBuilder{}

	for _, label := range ts.Labels {
		b.Add(label.Name, label.Value)

		if label.Name == labels.MetricName {
			metricName = label.Value
		} else {
			attrs.PutStr(label.Name, label.Value)
		}
	}

	b.Sort()

	return metricName, attrs, b.Labels().Hash()
}

// toTimeStamp converts timestamp in ms to OTLP timestamp in ns
func toTimeStamp(ts int64) (timestamp pcommon.Timestamp) {
	return pcommon.NewTimestampFromTime(time.UnixMilli(ts))
}
