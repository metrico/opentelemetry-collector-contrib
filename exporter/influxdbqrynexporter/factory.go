// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package influxdbqrynexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbqrynexporter"

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbqrynexporter/internal/metadata"
)

// NewFactory creates a factory for InfluxDB exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		HTTPClientSettings: confighttp.HTTPClientSettings{
			Timeout: 5 * time.Second,
			Headers: map[string]configopaque.String{
				"User-Agent": "OpenTelemetry -> Influx",
			},
		},
		QueueSettings:       exporterhelper.NewDefaultQueueSettings(),
		RetrySettings:       exporterhelper.NewDefaultRetrySettings(),
		MetricsSchema:       common.MetricsSchemaTelegrafPrometheusV1.String(),
		SpanDimensions:      otel2influx.DefaultOtelTracesToLineProtocolConfig().SpanDimensions,
		LogRecordDimensions: otel2influx.DefaultOtelLogsToLineProtocolConfig().LogRecordDimensions,
		// defaults per suggested:
		// https://docs.influxdata.com/influxdb/cloud-serverless/write-data/best-practices/optimize-writes/#batch-writes
		PayloadMaxLines: 10_000,
		PayloadMaxBytes: 10_000_000,
	}
}

func createTraceExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	config component.Config,
) (exporter.Traces, error) {
	cfg := config.(*Config)

	logger := newZapInfluxLogger(set.Logger)

	writer, err := newInfluxHTTPWriter(logger, cfg, set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	expConfig := otel2influx.DefaultOtelTracesToLineProtocolConfig()
	expConfig.Logger = logger
	expConfig.Writer = writer
	expConfig.SpanDimensions = cfg.SpanDimensions
	exp, err := otel2influx.NewOtelTracesToLineProtocol(expConfig)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		exp.WriteTraces,
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithRetry(cfg.RetrySettings),
		exporterhelper.WithStart(writer.Start),
	)
}

func createMetricsExporter(ctx context.Context, set exporter.CreateSettings, config component.Config) (exporter.Metrics, error) {
	cfg := config.(*Config)

	logger := newZapInfluxLogger(set.Logger)

	writer, err := newInfluxHTTPWriter(logger, cfg, set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	schema, found := common.MetricsSchemata[cfg.MetricsSchema]
	if !found {
		return nil, fmt.Errorf("schema '%s' not recognized", cfg.MetricsSchema)
	}

	expConfig := otel2influx.DefaultOtelMetricsToLineProtocolConfig()
	expConfig.Logger = logger
	expConfig.Writer = writer
	expConfig.Schema = schema
	exp, err := otel2influx.NewOtelMetricsToLineProtocol(expConfig)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		exp.WriteMetrics,
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithRetry(cfg.RetrySettings),
		exporterhelper.WithStart(writer.Start),
	)
}

func createLogsExporter(ctx context.Context, set exporter.CreateSettings, config component.Config) (exporter.Logs, error) {
	cfg := config.(*Config)

	logger := newZapInfluxLogger(set.Logger)

	writer, err := newInfluxHTTPWriter(logger, cfg, set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	b := writer.NewBatch()
	exp := OtelLogsToLineProtocol{
		influxHTTPWriterBatch: b.(*influxHTTPWriterBatch),
	}
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		exp.WriteLogs,
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithRetry(cfg.RetrySettings),
		exporterhelper.WithStart(writer.Start),
	)
}
