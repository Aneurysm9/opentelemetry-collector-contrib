// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sampling

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

func TestEvaluate_OTTL(t *testing.T) {
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})

	cases := []struct {
		Desc                string
		SpanConditions      []string
		SpanEventConditions []string
		Spans               []spanWithAttributes
		WantErr             bool
		Decision            Decision
	}{
		{
			// policy
			"OTTL conditions not set",
			[]string{},
			[]string{},
			[]spanWithAttributes{{SpanAttributes: map[string]string{"attr_k_1": "attr_v_1"}}},
			true,
			NotSampled,
		},
		{
			"OTTL conditions match specific span attributes 1",
			[]string{"attributes[\"attr_k_1\"] == \"attr_v_1\""},
			[]string{},
			[]spanWithAttributes{{SpanAttributes: map[string]string{"attr_k_1": "attr_v_1"}}},
			false,
			Sampled,
		},
		{
			"OTTL conditions match specific span attributes 2",
			[]string{"attributes[\"attr_k_1\"] != \"attr_v_1\""},
			[]string{},
			[]spanWithAttributes{{SpanAttributes: map[string]string{"attr_k_1": "attr_v_1"}}},
			false,
			NotSampled,
		},
		{
			"OTTL conditions inverse match(!=) span attributes 2",
			[]string{"attributes[\"attr_k_1\"] != \"attr_v_1\""},
			[]string{},
			[]spanWithAttributes{{SpanAttributes: map[string]string{"attr_k_1": "attr_v_2"}}},
			false,
			Sampled,
		},
		{
			"OTTL conditions match specific span event attributes",
			[]string{},
			[]string{"attributes[\"event_attr_k_1\"] == \"event_attr_v_1\""},
			[]spanWithAttributes{{SpanEventAttributes: map[string]string{"event_attr_k_1": "event_attr_v_1"}}},
			false,
			Sampled,
		},
		{
			"OTTL conditions match specific span event name",
			[]string{},
			[]string{"name != \"incorrect event name\""},
			[]spanWithAttributes{{SpanEventAttributes: nil}},
			false,
			Sampled,
		},
		{
			"OTTL conditions not matched",
			[]string{"attributes[\"attr_k_1\"] == \"attr_v_1\""},
			[]string{"attributes[\"event_attr_k_1\"] == \"event_attr_v_1\""},
			[]spanWithAttributes{},
			false,
			NotSampled,
		},
	}

	for _, c := range cases {
		t.Run(c.Desc, func(t *testing.T) {
			filter, err := NewOTTLConditionFilter(componenttest.NewNopTelemetrySettings(), c.SpanConditions, c.SpanEventConditions, ottl.IgnoreError)
			assert.Equal(t, err != nil, c.WantErr)

			if err == nil {
				decision, err := filter.Evaluate(context.Background(), traceID, newTraceWithSpansAttributes(c.Spans))
				assert.Equal(t, err != nil, c.WantErr)
				assert.Equal(t, decision, c.Decision)
			}
		})
	}
}

type spanWithAttributes struct {
	SpanAttributes      map[string]string
	SpanEventAttributes map[string]string
}

func newTraceWithSpansAttributes(spans []spanWithAttributes) *TraceData {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	ils := rs.ScopeSpans().AppendEmpty()

	for _, s := range spans {
		span := ils.Spans().AppendEmpty()
		span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
		for k, v := range s.SpanAttributes {
			span.Attributes().PutStr(k, v)
		}
		spanEvent := span.Events().AppendEmpty()
		spanEvent.SetName("test event")
		for k, v := range s.SpanEventAttributes {
			spanEvent.Attributes().PutStr(k, v)
		}
	}

	return &TraceData{
		ReceivedBatches: traces,
	}
}
