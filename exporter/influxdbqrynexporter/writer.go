// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package influxdbqrynexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbqrynexporter"

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/go-faster/city"
	_ "github.com/go-faster/city"
	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
)

var _ otel2influx.InfluxWriter = (*influxHTTPWriter)(nil)

type influxHTTPWriter struct {
	encoderPool sync.Pool
	httpClient  *http.Client

	httpClientSettings confighttp.HTTPClientSettings
	telemetrySettings  component.TelemetrySettings
	writeURL           string
	payloadMaxLines    int
	payloadMaxBytes    int

	logger common.Logger
}

func newInfluxHTTPWriter(logger common.Logger, config *Config, telemetrySettings component.TelemetrySettings) (*influxHTTPWriter, error) {
	writeURL, err := composeWriteURL(config)
	if err != nil {
		return nil, err
	}

	return &influxHTTPWriter{
		encoderPool: sync.Pool{
			New: func() interface{} {
				e := new(lineprotocol.Encoder)
				e.SetLax(false)
				e.SetPrecision(lineprotocol.Nanosecond)
				return e
			},
		},
		httpClientSettings: config.HTTPClientSettings,
		telemetrySettings:  telemetrySettings,
		writeURL:           writeURL,
		payloadMaxLines:    config.PayloadMaxLines,
		payloadMaxBytes:    config.PayloadMaxBytes,
		logger:             logger,
	}, nil
}

func composeWriteURL(config *Config) (string, error) {
	writeURL, err := url.Parse(config.HTTPClientSettings.Endpoint)
	if err != nil {
		return "", err
	}
	if writeURL.Path == "" || writeURL.Path == "/" {
		if config.V1Compatibility.Enabled {
			writeURL, err = writeURL.Parse("write")
			if err != nil {
				return "", err
			}
		} else {
			writeURL, err = writeURL.Parse("api/v2/write")
			if err != nil {
				return "", err
			}
		}
	}
	queryValues := writeURL.Query()
	queryValues.Set("precision", "ns")

	if config.V1Compatibility.Enabled {
		queryValues.Set("db", config.V1Compatibility.DB)

		if config.V1Compatibility.Username != "" && config.V1Compatibility.Password != "" {
			var basicAuth []byte
			base64.StdEncoding.Encode(basicAuth, []byte(config.V1Compatibility.Username+":"+string(config.V1Compatibility.Password)))
			config.HTTPClientSettings.Headers["Authorization"] = configopaque.String("Basic " + string(basicAuth))
		}
	} else {
		queryValues.Set("org", config.Org)
		queryValues.Set("bucket", config.Bucket)

		if config.Token != "" {
			config.HTTPClientSettings.Headers["Authorization"] = "Token " + config.Token
		}
	}

	writeURL.RawQuery = queryValues.Encode()

	return writeURL.String(), nil
}

// Start implements component.StartFunc
func (w *influxHTTPWriter) Start(_ context.Context, host component.Host) error {
	httpClient, err := w.httpClientSettings.ToClient(host, w.telemetrySettings)
	if err != nil {
		return err
	}
	w.httpClient = httpClient
	return nil
}

func (w *influxHTTPWriter) NewBatch() otel2influx.InfluxWriterBatch {
	return newInfluxHTTPWriterBatch(w)
}

var _ otel2influx.InfluxWriterBatch = (*influxHTTPWriterBatch)(nil)

type influxHTTPWriterBatch struct {
	*influxHTTPWriter
	encoder      *lineprotocol.Encoder
	payloadLines int
}

func newInfluxHTTPWriterBatch(w *influxHTTPWriter) *influxHTTPWriterBatch {
	return &influxHTTPWriterBatch{
		influxHTTPWriter: w,
	}
}

func hash128to64(lower64 uint64, higher64 uint64) uint64 {
	var kMul uint64 = 0x9ddfea08eb382d69
	var a = (lower64 ^ higher64) * kMul
	a ^= a >> 47
	var b = (higher64 ^ a) * kMul
	b ^= b >> 47
	b *= kMul
	return b
}

type OtelLogsToLineProtocol struct {
	*influxHTTPWriterBatch
	fingerprints map[string]bool
	attrs        map[string]string
	encoderMts   sync.Mutex
}

func (c *OtelLogsToLineProtocol) WriteLogs(ctx context.Context, ld plog.Logs) error {
	c.fingerprints = make(map[string]bool)
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLogs := ld.ResourceLogs().At(i)
		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			ilLogs := resourceLogs.ScopeLogs().At(j)
			err := c.mergeMaps(&c.attrs, ilLogs.Scope().Attributes(), resourceLogs.Resource().Attributes())
			if err != nil {
				return err
			}
			for k := 0; k < ilLogs.LogRecords().Len(); k++ {
				logRecord := ilLogs.LogRecords().At(k)
				if err := c.enqueueLogRecord(ctx, logRecord); err != nil {
					return consumererror.NewPermanent(fmt.Errorf("failed to convert OTLP log record to line protocol: %w", err))
				}
			}
		}
	}
	return c.writeBatch(ctx)
}

var attrSanitizer = regexp.MustCompile("[^a-zA-Z0-9_]")

func (c *OtelLogsToLineProtocol) mergeMaps(res *map[string]string, attrs ...pcommon.Map) error {
	for _, attr := range attrs {
		attr.Range(func(k string, v pcommon.Value) bool {
			(*res)[attrSanitizer.ReplaceAllString(k, "_")] = v.AsString()
			return true
		})
	}
	return nil
}

func (c *OtelLogsToLineProtocol) enqueueLogRecord(ctx context.Context, logRecord plog.LogRecord) error {
	ts := logRecord.Timestamp().AsTime()
	if ts.IsZero() {
		ts = time.Now()
	}

	attrs := make(map[string]string)
	for k, v := range c.attrs {
		attrs[k] = v
	}
	err := c.mergeMaps(&attrs, logRecord.Attributes())
	if err != nil {
		return err
	}

	if ots := logRecord.ObservedTimestamp().AsTime(); !ots.IsZero() && !ots.Equal(time.Unix(0, 0)) {
		ts = ots
	}

	if severityNumber := logRecord.SeverityNumber(); severityNumber != plog.SeverityNumberUnspecified {
		attrs[common.AttributeSeverityNumber] = strconv.FormatInt(int64(severityNumber), 10)
	}
	if severityText := logRecord.SeverityText(); severityText != "" {
		attrs[common.AttributeSeverityText] = severityText
	}
	body := logRecord.Body().AsString()
	if traceID, spanID := logRecord.TraceID(), logRecord.SpanID(); !traceID.IsEmpty() && !spanID.IsEmpty() {
		body = fmt.Sprintf("trace=%s span=%s %s", traceID.String(), spanID.String(), body)
		if name, ok := attrs["event.name"]; ok {
			body = fmt.Sprintf("%s %s", body, name)
			delete(attrs, "event.name")
		}
	}

	if err := c.enqueueLogRecordPoint(ctx, ts, attrs, body); err != nil {
		return fmt.Errorf("failed to write point for int gauge: %w", err)
	}

	return nil
}

func (c *OtelLogsToLineProtocol) hash(attrs map[string]string) uint64 {
	determs := [3]uint64{0, 0, 1}
	for k, v := range attrs {
		hash := hash128to64(
			city.CH64([]byte(k)),
			city.CH64([]byte(v)),
		)
		determs[0] = determs[0] + hash
		determs[1] = determs[1] ^ hash
		determs[2] = determs[2] * (1779033703 + 2*hash)
	}
	return hash128to64(determs[0], hash128to64(determs[1], determs[2]))
}

func jsonFastMarshal(m map[string]string) []byte {
	var a bytes.Buffer
	j := 0
	a.Write([]byte{'{'})
	for k, v := range m {
		if j != 0 {
			a.Write([]byte{','})
		}
		a.Write([]byte(strconv.Quote(k)))
		a.Write([]byte{':'})
		a.Write([]byte(strconv.Quote(v)))
		j++
	}
	a.Write([]byte{'}'})
	return a.Bytes()
}

func (c *OtelLogsToLineProtocol) enqueueLogRecordPoint(ctx context.Context, ts time.Time,
	attrs map[string]string, body string) error {
	if c.encoder == nil {
		c.encoder = c.encoderPool.Get().(*lineprotocol.Encoder)
	}
	c.encoder.StartLine("logs_v2")
	hash := c.hash(attrs)
	attrTags := make([]string, 0, len(attrs))
	for k := range attrs {
		attrTags = append(attrTags, k)
	}
	sort.Strings(attrTags)
	c.encoder.AddTag("fingerprint", strconv.FormatUint(hash, 16))
	for _, k := range attrTags {
		_v, _ := lineprotocol.StringValue(attrs[k])
		c.encoder.AddField("attributes."+k, _v)
	}
	val, _ := lineprotocol.StringValue(body)
	c.encoder.AddField("body", val)
	c.encoder.EndLine(ts)
	if c.encoder.Err() != nil {
		return c.encoder.Err()
	}

	day := ts.Truncate(time.Hour * 24)
	if !c.fingerprints[fmt.Sprintf("d-%s-%d", day.Format(time.DateOnly), hash)] {
		c.fingerprints[fmt.Sprintf("d-%s-%d", day.Format(time.DateOnly), hash)] = true
		iK := hash >> 19
		_ts := ts.Truncate(time.Hour * 24).Add(time.Duration(iK) * time.Nanosecond)
		c.encoder.StartLine("logs.stream.daily.2")
		c.encoder.AddTag("fingerprint", strconv.FormatUint(hash, 16))
		for _, k := range attrTags {
			_v, _ := lineprotocol.StringValue(attrs[k])
			c.encoder.AddField("attributes."+k, _v)
		}
		keys, _ := json.Marshal(attrTags)
		_v, _ := lineprotocol.StringValueFromBytes(keys)
		c.encoder.AddField("keys", _v)
		c.encoder.EndLine(_ts)
	}
	return c.encoder.Err()
}

func (c *OtelLogsToLineProtocol) writeBatch(ctx context.Context) error {
	if c.encoder == nil {
		return nil
	}

	defer func() {
		c.encoder.Reset()
		c.encoder.ClearErr()
		c.encoderPool.Put(c.encoder)
		c.encoder = nil
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.writeURL, bytes.NewReader(c.encoder.Bytes()))
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	if res, err := c.httpClient.Do(req); err != nil {
		return err
	} else if body, err := io.ReadAll(res.Body); err != nil {
		return err
	} else if err = res.Body.Close(); err != nil {
		return err
	} else {
		switch res.StatusCode / 100 {
		case 2: // Success
			break
		case 5: // Retryable error
			return fmt.Errorf("line protocol write returned %q %q", res.Status, string(body))
		default: // Terminal error
			return consumererror.NewPermanent(fmt.Errorf("line protocol write returned %q %q", res.Status, string(body)))
		}
	}

	return nil
}

// EnqueuePoint emits a set of line protocol attributes (metrics, tags, fields, timestamp)
// to the internal line protocol buffer.
// If the buffer is full, it will be flushed by calling WriteBatch.
func (b *influxHTTPWriterBatch) EnqueuePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, _ common.InfluxMetricValueType) error {
	if b.encoder == nil {
		b.encoder = b.encoderPool.Get().(*lineprotocol.Encoder)
	}

	b.encoder.StartLine(measurement)
	determs := [3]uint64{0, 0, 1}

	_tags := b.optimizeTags(tags)
	for _, tag := range _tags {
		b.encoder.AddTag(tag.k, tag.v)
	}

	attrs := make(map[string]string)

	b.encoder.EndLine(ts)

	/* Generate indexes */

	if measurement == "logs" {
		hash := hash128to64(
			hash128to64(determs[0], determs[1]),
			determs[2],
		)
		hash &= 0x3FFFFFFFFFFF
		for k, v := range attrs {
			b.encoder.StartLine("time_series_gin_3")
			b.encoder.AddTag("key", k)
			b.encoder.AddTag("val", v)
			f, _ := lineprotocol.StringValue(k + ":" + v)
			b.encoder.AddField("f", f)
			b.encoder.EndLine(time.Unix(0, time.Now().Truncate(time.Hour*24).UnixNano()+int64(hash)))
		}
	}

	if err := b.encoder.Err(); err != nil {
		b.encoder.Reset()
		b.encoder.ClearErr()
		b.encoderPool.Put(b.encoder)
		b.encoder = nil
		return consumererror.NewPermanent(fmt.Errorf("failed to encode point: %w", err))
	}

	b.payloadLines++
	if b.payloadLines >= b.payloadMaxLines || len(b.encoder.Bytes()) >= b.payloadMaxBytes {
		if err := b.WriteBatch(ctx); err != nil {
			return err
		}
	}

	return nil
}

// WriteBatch sends the internal line protocol buffer to InfluxDB.
func (b *influxHTTPWriterBatch) WriteBatch(ctx context.Context) error {
	if b.encoder == nil {
		return nil
	}

	defer func() {
		b.encoder.Reset()
		b.encoder.ClearErr()
		b.encoderPool.Put(b.encoder)
		b.encoder = nil
		b.payloadLines = 0
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.writeURL, bytes.NewReader(b.encoder.Bytes()))
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	if res, err := b.httpClient.Do(req); err != nil {
		return err
	} else if body, err := io.ReadAll(res.Body); err != nil {
		return err
	} else if err = res.Body.Close(); err != nil {
		return err
	} else {
		switch res.StatusCode / 100 {
		case 2: // Success
			break
		case 5: // Retryable error
			return fmt.Errorf("line protocol write returned %q %q", res.Status, string(body))
		default: // Terminal error
			return consumererror.NewPermanent(fmt.Errorf("line protocol write returned %q %q", res.Status, string(body)))
		}
	}

	return nil
}

type tag struct {
	k, v string
}

// optimizeTags sorts tags by key and removes tags with empty keys or values
func (b *influxHTTPWriterBatch) optimizeTags(m map[string]string) []tag {
	tags := make([]tag, 0, len(m))
	for k, v := range m {
		switch {
		case k == "":
			b.logger.Debug("empty tag key")
		case v == "":
			b.logger.Debug("empty tag value", "key", k)
		default:
			tags = append(tags, tag{k, v})
		}
	}
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].k < tags[j].k
	})
	return tags
}

func (b *influxHTTPWriterBatch) convertFields(m map[string]interface{}) (fields map[string]lineprotocol.Value) {
	fields = make(map[string]lineprotocol.Value, len(m))
	for k, v := range m {
		if k == "" {
			b.logger.Debug("empty field key")
		} else if lpv, ok := lineprotocol.NewValue(v); !ok {
			b.logger.Debug("invalid field value", "key", k, "value", v)
		} else {
			fields[k] = lpv
		}
	}
	return
}
