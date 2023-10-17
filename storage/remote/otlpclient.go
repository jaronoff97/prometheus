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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	grpcExporter "go.opentelemetry.io/collector/exporter/otlpexporter"
)

type OtlpClient struct {
	remoteName string // Used to differentiate clients in metrics.
	urlString  string // url.String()
	Client     exporter.Metrics
	timeout    time.Duration

	retryOnRateLimit bool

	readQueries         prometheus.Gauge
	readQueriesTotal    *prometheus.CounterVec
	readQueriesDuration prometheus.Observer
}

func (o OtlpClient) StoreSamples(ctx context.Context, series []prompb.TimeSeries, i int) error {
	//TODO implement me
	panic("implement me")
}

func (o OtlpClient) StoreMetadata(ctx context.Context, metadata []prompb.MetricMetadata, i int) error {
	//TODO implement me
	panic("implement me")
}

func (o OtlpClient) Name() string {
	return o.remoteName
}

func (o OtlpClient) Endpoint() string {
	return o.urlString
}

// NewOtlpWriteClient creates a new client for remote write via OTLP.
func NewOtlpWriteClient(name string, conf *ClientConfig) (WriteClient, error) {
	createSettings, exporterSettings := createSettingsFromClientConfig(name, conf)
	metricsExporter, err := grpcExporter.NewFactory().CreateMetricsExporter(context.Background(), createSettings, exporterSettings)
	if err != nil {
		return nil, err
	}

	return &OtlpClient{
		remoteName:       name,
		urlString:        conf.URL.String(),
		Client:           metricsExporter,
		retryOnRateLimit: conf.RetryOnRateLimit,
		timeout:          time.Duration(conf.Timeout),
	}, nil
}

func createSettingsFromClientConfig(name string, conf *ClientConfig) (exporter.CreateSettings, grpcExporter.Config) {
	createSettings := exporter.CreateSettings{
		ID: component.NewID("otlp/prometheuswriter"),
		// TODO: set otel tracer here
		TelemetrySettings: component.TelemetrySettings{},
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
	exporterSettings := grpcExporter.Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutSettings(),
		QueueSettings:   exporterhelper.NewDefaultQueueSettings(),
		RetrySettings:   exporterhelper.NewDefaultRetrySettings(),
		GRPCClientSettings: configgrpc.GRPCClientSettings{
			Endpoint: conf.URL.String(),
			Headers:  headers,
		},
	}
	return createSettings, exporterSettings
}
