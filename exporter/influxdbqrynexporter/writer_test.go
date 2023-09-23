// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package influxdbqrynexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkJsonMarshal(b *testing.B) {
	attrs := map[string]string{
		"a": "b",
		"b": "c",
		"d": "e",
		"f": "g",
		"h": "i",
	}
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(attrs)
	}
}

func _jsonFastMarshal(m map[string]string) []byte {
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

func BenchmarkJsonFastMarshal2(b *testing.B) {
	attrs := map[string]string{
		"a": "b",
		"b": "c",
		"d": "e",
		"f": "g",
		"h": "i",
	}
	for i := 0; i < b.N; i++ {
		_jsonFastMarshal(attrs)
	}
}

func TestBK(t *testing.T) {
	for k, v := range map[string]string{"a": "b", "qwaszxer": "1", "qazxswedcvfr": "2"} {
		bK := [8]byte{}
		copy(bK[:], k)
		for i := 0; i < 4; i++ {
			bK[i], bK[7-i] = bK[7-i], bK[i]
		}
		iK := *((*uint64)(unsafe.Pointer(&bK[0])))
		iK = iK >> 14
		fmt.Println(iK, v)
	}
}

func Test_influxHTTPWriterBatch_optimizeTags(t *testing.T) {
	batch := &influxHTTPWriterBatch{
		influxHTTPWriter: &influxHTTPWriter{
			logger: common.NoopLogger{},
		},
	}

	for _, testCase := range []struct {
		name         string
		m            map[string]string
		expectedTags []tag
	}{
		{
			name:         "empty map",
			m:            map[string]string{},
			expectedTags: []tag{},
		},
		{
			name: "one tag",
			m: map[string]string{
				"k": "v",
			},
			expectedTags: []tag{
				{"k", "v"},
			},
		},
		{
			name: "empty tag key",
			m: map[string]string{
				"": "v",
			},
			expectedTags: []tag{},
		},
		{
			name: "empty tag value",
			m: map[string]string{
				"k": "",
			},
			expectedTags: []tag{},
		},
		{
			name: "seventeen tags",
			m: map[string]string{
				"k00": "v00", "k01": "v01", "k02": "v02", "k03": "v03", "k04": "v04", "k05": "v05", "k06": "v06", "k07": "v07", "k08": "v08", "k09": "v09", "k10": "v10", "k11": "v11", "k12": "v12", "k13": "v13", "k14": "v14", "k15": "v15", "k16": "v16",
			},
			expectedTags: []tag{
				{"k00", "v00"}, {"k01", "v01"}, {"k02", "v02"}, {"k03", "v03"}, {"k04", "v04"}, {"k05", "v05"}, {"k06", "v06"}, {"k07", "v07"}, {"k08", "v08"}, {"k09", "v09"}, {"k10", "v10"}, {"k11", "v11"}, {"k12", "v12"}, {"k13", "v13"}, {"k14", "v14"}, {"k15", "v15"}, {"k16", "v16"},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gotTags := batch.optimizeTags(testCase.m)
			assert.Equal(t, testCase.expectedTags, gotTags)
		})
	}
}

func Test_influxHTTPWriterBatch_maxPayload(t *testing.T) {
	for _, testCase := range []struct {
		name            string
		payloadMaxLines int
		payloadMaxBytes int

		expectMultipleRequests bool
	}{{
		name:            "default",
		payloadMaxLines: 10_000,
		payloadMaxBytes: 10_000_000,

		expectMultipleRequests: false,
	}, {
		name:            "limit-lines",
		payloadMaxLines: 1,
		payloadMaxBytes: 10_000_000,

		expectMultipleRequests: true,
	}, {
		name:            "limit-bytes",
		payloadMaxLines: 10_000,
		payloadMaxBytes: 1,

		expectMultipleRequests: true,
	}} {
		t.Run(testCase.name, func(t *testing.T) {
			var httpRequests []*http.Request

			mockHTTPService := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpRequests = append(httpRequests, r)
			}))
			t.Cleanup(mockHTTPService.Close)

			batch := &influxHTTPWriterBatch{
				influxHTTPWriter: &influxHTTPWriter{
					encoderPool: sync.Pool{
						New: func() interface{} {
							e := new(lineprotocol.Encoder)
							e.SetLax(false)
							e.SetPrecision(lineprotocol.Nanosecond)
							return e
						},
					},
					httpClient:      &http.Client{},
					writeURL:        mockHTTPService.URL,
					payloadMaxLines: testCase.payloadMaxLines,
					payloadMaxBytes: testCase.payloadMaxBytes,
					logger:          common.NoopLogger{},
				},
			}

			err := batch.EnqueuePoint(context.Background(), "m", map[string]string{"k": "v"}, map[string]interface{}{"f": int64(1)}, time.Unix(1, 0), 0)
			require.NoError(t, err)
			err = batch.EnqueuePoint(context.Background(), "m", map[string]string{"k": "v"}, map[string]interface{}{"f": int64(2)}, time.Unix(2, 0), 0)
			require.NoError(t, err)
			err = batch.WriteBatch(context.Background())
			require.NoError(t, err)

			if testCase.expectMultipleRequests {
				assert.Equal(t, 2, len(httpRequests))
			} else {
				assert.Equal(t, 1, len(httpRequests))
			}
		})
	}
}
