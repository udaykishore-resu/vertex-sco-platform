// Package tracing gives every Vertex service a way to correlate one
// shopper transaction across the terminal -> store -> cloud hop, directly
// addressing "no end-to-end tracing story" from the architecture review.
//
// It is intentionally implemented against the W3C Trace Context format
// (https://www.w3.org/TR/trace-context/) — the same TraceID/SpanID model
// OpenTelemetry uses — so this shim is a drop-in seam: swap the Exporter
// interface's default implementation for go.opentelemetry.io/otel's OTLP
// exporter when the project has network access to fetch it, without
// touching any call site. That trade-off (dependency-free by default, one
// interface away from real OTel) is documented in docs/ARCHITECTURE.md.
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
)

type ctxKey struct{}

// Span represents one unit of work. Call End() when done.
type Span struct {
	TraceID   string
	SpanID    string
	ParentID  string
	Name      string
	Service   string
	StartedAt time.Time
	exporter  Exporter
}

// Exporter is where finished spans go. StdoutExporter (below) logs JSON
// lines; a production deployment points this at an OTLP collector (see
// deploy/otel/otel-collector-config.yaml, which is ready to receive OTLP and
// forward to Jaeger — only the Go-side exporter implementation changes).
type Exporter interface {
	Export(FinishedSpan)
}

type FinishedSpan struct {
	TraceID    string    `json:"trace_id"`
	SpanID     string    `json:"span_id"`
	ParentID   string    `json:"parent_id,omitempty"`
	Name       string    `json:"name"`
	Service    string    `json:"service"`
	StartedAt  time.Time `json:"started_at"`
	DurationMs float64   `json:"duration_ms"`
	Error      string    `json:"error,omitempty"`
}

type StdoutExporter struct{}

func (StdoutExporter) Export(s FinishedSpan) {
	b, _ := json.Marshal(s)
	log.Printf("[trace] %s", string(b))
}

var defaultExporter Exporter = StdoutExporter{}

// SetExporter overrides the package-level exporter (call once at service
// startup, e.g. main() wiring an OTLP exporter in a full deployment).
func SetExporter(e Exporter) { defaultExporter = e }

func newID(bytes int) string {
	b := make([]byte, bytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// StartRoot begins a new trace (used at the terminal, where a transaction
// originates).
func StartRoot(ctx context.Context, service, name string) (context.Context, *Span) {
	s := &Span{
		TraceID:   newID(16),
		SpanID:    newID(8),
		Name:      name,
		Service:   service,
		StartedAt: time.Now(),
		exporter:  defaultExporter,
	}
	return context.WithValue(ctx, ctxKey{}, s), s
}

// StartChild continues an existing trace using a TraceContext extracted
// from an inbound event envelope or HTTP header — this is what lets
// vertex-intervention's span nest under the vertex-core span that triggered
// it, even though they communicate over MQTT rather than a direct call.
func StartChild(ctx context.Context, service, name string, parent domain.TraceContext) (context.Context, *Span) {
	traceID := parent.TraceID
	if traceID == "" {
		traceID = newID(16)
	}
	s := &Span{
		TraceID:   traceID,
		SpanID:    newID(8),
		ParentID:  parent.SpanID,
		Name:      name,
		Service:   service,
		StartedAt: time.Now(),
		exporter:  defaultExporter,
	}
	return context.WithValue(ctx, ctxKey{}, s), s
}

// FromContext retrieves the active span, if any.
func FromContext(ctx context.Context) (*Span, bool) {
	s, ok := ctx.Value(ctxKey{}).(*Span)
	return s, ok
}

// Inject produces a domain.TraceContext to embed in an outgoing envelope so
// the next service can StartChild from it.
func (s *Span) Inject() domain.TraceContext {
	if s == nil {
		return domain.TraceContext{}
	}
	return domain.TraceContext{TraceID: s.TraceID, SpanID: s.SpanID, ParentSpanID: s.ParentID}
}

// End finalizes the span and exports it. err, if non-nil, is recorded.
func (s *Span) End(err error) {
	if s == nil {
		return
	}
	fs := FinishedSpan{
		TraceID:    s.TraceID,
		SpanID:     s.SpanID,
		ParentID:   s.ParentID,
		Name:       s.Name,
		Service:    s.Service,
		StartedAt:  s.StartedAt,
		DurationMs: float64(time.Since(s.StartedAt).Microseconds()) / 1000.0,
	}
	if err != nil {
		fs.Error = err.Error()
	}
	s.exporter.Export(fs)
}

// TraceparentHeader formats the span as a W3C traceparent header value, for
// services that also expose HTTP (e.g. vertex-config, vertex-control-plane)
// so trace context propagates across both MQTT and HTTP hops uniformly.
func (s *Span) TraceparentHeader() string {
	if s == nil {
		return ""
	}
	return "00-" + s.TraceID + "-" + s.SpanID + "-01"
}
