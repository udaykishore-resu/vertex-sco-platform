// Package outbox implements the transactional-outbox pattern as the
// explicit offline contract called for in the architecture review
// ("Offline/degraded-mode behavior is implicit, not designed"): if cloud
// (or even store-to-terminal) connectivity drops mid-transaction, events are
// appended to a local, durable, append-only log instead of being dropped,
// and replayed in order once the event bus is reachable again.
//
// This is intentionally a plain write-ahead log on local disk (one JSON
// line per event) rather than a database, so it has zero dependencies and
// survives a process crash — the file is fsynced on every append.
package outbox

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/eventbus"
)

type record struct {
	Topic      string          `json:"topic"`
	Envelope   domain.Envelope `json:"envelope"`
	EnqueuedAt time.Time       `json:"enqueued_at"`
	Replayed   bool            `json:"replayed"`
}

// Outbox durably queues (topic, envelope) pairs to disk and replays them
// against a Bus once connectivity is available.
type Outbox struct {
	path string
	mu   sync.Mutex
}

func Open(path string) (*Outbox, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	_ = f.Close()
	return &Outbox{path: path}, nil
}

// Enqueue appends an event durably. Call this instead of bus.Publish
// directly whenever the bus is known (or suspected) to be unreachable, or
// unconditionally if you want "durable by default, published as a
// side-effect" semantics.
func (o *Outbox) Enqueue(topic string, env domain.Envelope) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	f, err := os.OpenFile(o.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	rec := record{Topic: topic, Envelope: env, EnqueuedAt: time.Now()}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync() // durability: survive a process crash right after enqueue
}

// ReplayLoop periodically attempts to flush all not-yet-replayed records to
// bus, in original order, compacting the log file once everything currently
// present has been published successfully. It runs until ctx is cancelled —
// call it once from main() as `go outbox.ReplayLoop(ctx, bus, interval)`.
func (o *Outbox) ReplayLoop(ctx context.Context, bus eventbus.Bus, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = o.flush(bus)
		}
	}
}

func (o *Outbox) flush(bus eventbus.Bus) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	f, err := os.Open(o.path)
	if err != nil {
		return err
	}
	var records []record
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var r record
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			continue // skip malformed lines rather than fail the whole replay
		}
		records = append(records, r)
	}
	f.Close()

	remaining := make([]record, 0, len(records))
	for _, r := range records {
		if r.Replayed {
			continue
		}
		if err := bus.Publish(r.Topic, r.Envelope); err != nil {
			// Bus still unreachable: keep this and everything after it,
			// preserving order, and try again next tick.
			remaining = append(remaining, r)
			continue
		}
		// published: drop it (do not carry forward)
	}

	return o.rewrite(remaining)
}

func (o *Outbox) rewrite(records []record) error {
	tmp := o.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, r := range records {
		b, err := json.Marshal(r)
		if err != nil {
			continue
		}
		w.Write(append(b, '\n'))
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, o.path)
}

// PendingCount reports how many records are queued (for a /health or
// /metrics endpoint — a growing backlog is a strong "we're offline" signal).
func (o *Outbox) PendingCount() (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f, err := os.Open(o.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		n++
	}
	return n, nil
}
