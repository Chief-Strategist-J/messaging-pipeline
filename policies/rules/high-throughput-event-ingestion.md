# High-Throughput Event Ingestion & Stream Processing Architecture

*Standalone architecture — no relation to the Next.js/Python documents. Target: **1,000,000 requests in 10 minutes, on minimum resources.** Built like a performance engineer would build it: numbers before code, language chosen per tier by what that tier actually needs, and every "minimum resources" claim tied to a measurement, not an assertion.*

## Table of Contents

1. [The Performance Budget — Math Before Code](#1-the-performance-budget--math-before-code)
2. [Language & Technology Choices, Reasoned](#2-language--technology-choices-reasoned)
3. [Data-Driven & Event-Driven Design](#3-data-driven--event-driven-design)
4. [System Architecture](#4-system-architecture)
5. [Folder Structure](#5-folder-structure)
6. [Event Schema — Avro + Schema Registry](#6-event-schema--avro--schema-registry)
7. [Ingestion Service (Go) — Complete Code](#7-ingestion-service-go--complete-code)
8. [Kafka — Topic & Partition Sizing](#8-kafka--topic--partition-sizing)
9. [Kafka Connect — Sink to Postgres](#9-kafka-connect--sink-to-postgres)
10. [Kafka Streams (Kotlin) — Complete Code](#10-kafka-streams-kotlin--complete-code)
11. [Redis & Postgres — Scope](#11-redis--postgres--scope)
12. [Observability](#12-observability)
13. [Load Testing — Proving the Target](#13-load-testing--proving-the-target)
14. [Full docker-compose.yml](#14-full-docker-composeyml)
15. [Kubernetes — Sizing From Measurement, Not Guesses](#15-kubernetes--sizing-from-measurement-not-guesses)
16. [Scope Boundaries](#16-scope-boundaries)
17. [References](#17-references)

---

## 1. The Performance Budget — Math Before Code

**Target**: 1,000,000 requests / 600 seconds = **1,667 req/s sustained average**.

A performance engineer doesn't provision for the average — bursts happen, retries happen, GC/scheduler jitter happens. Design for **3x peak headroom: ~5,000 req/s**.

**What actually happens per request** (this determines everything downstream): parse a small JSON body (~200–500 bytes) → optional Redis dedup check (local-network round trip, sub-millisecond) → produce to Kafka. The Kafka produce is the only part that touches another network hop, and it doesn't have to be slow: a Kafka producer **batches** many concurrent produce calls together within a short `linger.ms` window and sends them as one broker round trip — so 1,000 concurrent goroutines calling produce don't cost 1,000 separate network round trips, they cost however many *batches* fit in that window.

**Honest calibration, not a fabricated benchmark**: published comparisons show Go's fastest Kafka clients are the most CPU/memory-efficient in the Go ecosystem, and a goroutine costs ~2KB vs. ~8MB for an OS thread — meaning thousands of concurrent in-flight requests cost megabytes, not gigabytes, of memory. Rather than assert a precise "requests per core" number here (that number is workload- and hardware-specific and would be exactly the kind of unverified claim to distrust), §13 gives you a load test that measures your actual per-instance capacity directly. The architecture below is designed so that even a conservative real-world number — a couple thousand req/s per small instance — clears the 5,000 req/s peak target with 2–3 instances, not a fleet.

**That's the core performance-engineering insight for this task**: 1,667 req/s sustained is a *modest* number. It doesn't need a large cluster — it needs a *correctly shaped* thin ingestion tier (async I/O, cheap concurrency primitive, batched downstream writes). Most of the "minimum resources" win here comes from not fighting the language's concurrency model, not from exotic infrastructure.

---

## 2. Language & Technology Choices, Reasoned

| Tier | Language | Why | Alternatives considered |
|---|---|---|---|
| **Ingestion API** (§7) | **Go** | Goroutines are ~2KB each vs. ~8MB per OS thread — thousands of concurrent requests cost megabytes. Compiles to a static binary; a distroless image is ~15–20MB with no runtime dependency. Baseline memory for a simple service is tens of MB, not hundreds. | **Rust**: marginally better raw efficiency, but 3–5x the development time for a target Go clears with room to spare — that's complexity you'd be paying for without needing it. **Node.js**: single-threaded event loop needs explicit clustering to use more than one core, and per-connection memory is higher than Go's. **Python (even async)**: the GIL caps true parallelism per process, so matching Go's throughput needs several times more replicas — directly working against "minimum resources." |
| **Stream Processing** (§10) | **Kotlin (JVM)** | Kafka Streams *is* a JVM library — there is no non-JVM version of the real thing. Kotlin gives modern, concise syntax over the exact same DSL and runtime Java uses; nothing is lost or reimplemented. | Hand-rolling stream semantics in Go/Rust — you'd lose exactly-once processing, windowing, fault-tolerant state stores, and interactive queries that Kafka Streams gives you for free. Not a reasonable trade for this. |
| **Kafka Connect connectors** (§9) | **Existing JVM plugins, no code** | Standard connectors (JDBC sink, S3 sink) are configuration, not code you write. | Custom connector — only justified if no existing plugin fits, which is rare for "write processed data to Postgres." |
| Kafka, Postgres, Redis, OTel Collector, Prometheus, Grafana, Tempo | — (infra) | Unchanged in role from prior architecture work — still the right tools for buffering, storage, cache, and observability. | — |

The one-sentence version: **pick the lightest-weight language where the whole job is "receive, validate, forward" (Go), and use the one language where the *real* stream-processing library actually lives (Kotlin/JVM) — don't force a single language across tiers that have fundamentally different jobs.**

---

## 3. Data-Driven & Event-Driven Design

**A correction to the previous answer first.** When asked whether this was data-driven, the response framed the ingestion handler and topology as "hardcoded on purpose... to shave cycles." On reflection, that overstated the tradeoff. The actual cost breakdown:

| What "data-driven" means here | Runtime cost | Why |
|---|---|---|
| Validation rules as data (§7) | **Negligible** — nanoseconds | Walking a short slice of field-presence checks is not meaningfully different from hardcoded `if` statements. The request's real cost is the millisecond-scale Kafka/Redis network I/O, next to which this is noise. |
| Topology shape as data (§10) | **Zero at steady state** | A Kafka Streams topology is built **once**, at JVM startup, from whatever data structure describes it. Kafka Streams then executes the compiled processor graph exactly the same way regardless of whether that graph was hand-written or generated — there is no per-record difference. |
| Live, async, network-dependent rules-as-data (e.g. the Next.js rules engine's `asyncCheck`) | **Real, non-zero** | This genuinely doesn't belong in this hot path — a rule that makes an external call per request *would* cost real latency. This document doesn't use that pattern anywhere. |

So: the redesign below makes both tiers properly data-driven, and durability/speed are unaffected — not because they were sacrificed, but because the two things that actually determine "durability and speed" here (`acks=all`, idempotent-leaning production, `EXACTLY_ONCE_V2`, `upsert` sinks, batching) live **underneath** the data-driven layer and are untouched by it.

**The design rule this document now follows everywhere:**

> Separate what changes **without a deploy** (data) from what changes **with one** (code) — then write exactly one generic engine per axis of variability, never one handler per case.

Concretely:
- **Data**: which event types exist, what each one's fields must satisfy, which topic it routes to, which topology steps run in what order and with what parameters.
- **Code**: the generic engine that interprets that data (one HTTP handler, one topology builder), plus registered building blocks (`FieldRule` checks, topology step types, custom processors) for the cases that are genuinely novel, not just a new instance of an existing shape.
- **The seam between them is always the same shape**: a `map` plus a `register()` function plus a `get-or-fail-loudly()` lookup. This is the exact pattern used for field renderers, async checkers, and workflow steps in prior architecture work — carried here into Go and Kotlin because the shape is language-agnostic, even though the syntax isn't.
- **Config fails fast, at startup, before serving traffic** — never as a surprise on request #40,000. A typo in `event-types.yaml` should crash the pod on boot, not silently misvalidate events for an hour.
- **Data stays strongly typed.** `map[string]any`/`Map<String, Any>` soup that throws away compiler help is a code-quality regression, not a data-driven win. Every config shape below is a real Go struct or Kotlin data class — it's externally configurable, not untyped.

This is also what makes it **event-driven**, not just data-driven: because publishers (the ingestion service) and subscribers (the streams app, and any future consumer) only ever agree on a topic and a schema — never on each other's existence — adding an entirely new downstream service (fraud scoring, analytics, a second aggregation) is *zero-touch* on everything upstream. That property, not any single file below, is the actual "10 lines now, 500 lines of leverage later" payoff.

---

## 4. System Architecture

```
                         ┌───────────────────────────────┐
  client burst           │         Load Balancer          │
  (1M req / 10 min) ────▶│        (L4/L7, stateless)       │
                         └───────────────┬─────────────────┘
                                         │  N replicas, horizontally scaled
                         ┌───────────────▼─────────────────┐
                         │   Ingestion Service (Go)         │
                         │   validate → dedup (Redis) →     │
                         │   produce (batched, idempotent)  │
                         └───────────────┬─────────────────┘
                                         │ Avro (Schema Registry)
                         ┌───────────────▼─────────────────┐
                         │   Kafka topic: events.raw         │
                         │   (KRaft, 12 partitions — §8)      │
                         └──────┬──────────────────┬─────────┘
                                │                  │
                ┌───────────────▼───┐   ┌───────────▼─────────────┐
                │ Kafka Streams       │   │  Kafka Connect            │
                │ (Kotlin)            │   │  Sink: events.raw          │
                │ dedup / windowed    │   │  → Postgres (raw archive)  │
                │ aggregation         │   └────────────────────────────┘
                │ → events.enriched   │
                └──────────┬──────────┘
                           │
                ┌──────────▼──────────────┐
                │ Kafka Connect              │
                │ Sink: events.enriched      │
                │ → Postgres (aggregates)    │
                └─────────────────────────────┘

   Every tier exports OTLP → OTel Collector → Prometheus + Tempo → Grafana (§12)
```

---

## 5. Folder Structure

```
event-platform/
├── services/
│   ├── ingestion-api/                     # Go — the 1M-req/10min front door
│   │   ├── cmd/server/main.go
│   │   ├── internal/
│   │   │   ├── httpapi/
│   │   │   │   ├── handler.go             # ONE handler — generic over every event type
│   │   │   │   └── middleware.go          # tracing + load-shedding decorators
│   │   │   ├── eventtypes/                # the data-driven core — §3, §7
│   │   │   │   ├── config.go              # EventTypeConfig, FieldRule — typed, not map[string]any
│   │   │   │   ├── registry.go            # Map + register() + Get() — fail-fast on load
│   │   │   │   ├── loader.go              # reads event-types.yaml
│   │   │   │   ├── validate.go            # generic FieldRule interpreter
│   │   │   │   └── custom_processors.go   # registry escape hatch for non-declarative logic
│   │   │   ├── customprocessors/
│   │   │   │   └── purchase.go            # the ONE file needed because "purchase" isn't purely declarative
│   │   │   ├── ingest/
│   │   │   │   ├── event.go               # RawEvent envelope — fixed & typed, on purpose
│   │   │   │   ├── dedup.go               # Redis-backed idempotency
│   │   │   │   ├── avro.go                # Confluent wire-format encode
│   │   │   │   └── producer.go            # Kafka producer, topic-as-argument, decorator pattern
│   │   │   ├── config/config.go
│   │   │   └── observability/otel.go
│   │   ├── go.mod
│   │   └── Dockerfile                     # multi-stage, distroless static
│   │
│   └── stream-processor/                  # Kotlin — Kafka Streams topology
│       ├── src/main/kotlin/com/platform/streams/
│       │   ├── Application.kt
│       │   ├── topology/
│       │   │   ├── TopologyDefinition.kt      # the data: steps, config, sink — §10
│       │   │   ├── StepRegistry.kt            # Map + register() — same shape as eventtypes
│       │   │   ├── BuiltinSteps.kt            # registered step TYPES: dedup, filterByType, ...
│       │   │   ├── TopologyBuilder.kt         # walks TopologyDefinition, built ONCE at startup
│       │   │   └── processors/
│       │   │       └── ProcessorRegistry.kt   # ported from the Next.js/Python design, unchanged shape
│       │   └── serde/AvroSerdes.kt
│       ├── build.gradle.kts
│       └── Dockerfile
│
├── schemas/avro/
│   ├── event-raw.avsc
│   └── event-enriched.avsc
│
├── config/
│   └── event-types.yaml                   # THE leverage point — new event types live here, not in Go code
│
├── infra/
│   ├── docker-compose.yml                 # §14
│   ├── kafka/connectors/
│   │   ├── postgres-raw-sink.json
│   │   └── postgres-enriched-sink.json
│   ├── otel-collector/otel-collector-config.yaml
│   ├── prometheus/prometheus.yml
│   ├── tempo/tempo.yaml
│   ├── grafana/provisioning/
│   └── k8s/
│       ├── ingestion-api/ (Deployment + HPA — §15)
│       ├── stream-processor/ (Deployment)
│       └── kafka/ (Strimzi CR)
│
├── loadtest/
│   └── ingestion_burst.js                 # k6 — proves the 1M/10min target, §13
│
└── Makefile
```

Go gets Go's own idiomatic layout (`cmd/`, `internal/`) rather than the `packages/` convention from the Next.js work — "same development rigor," not "same folder names copied onto a language where they'd be unidiomatic."

---

## 6. Event Schema — Avro + Schema Registry

JSON-on-Kafka is the easy default and the wrong one here: Avro payloads are smaller (less network + less broker disk, both directly serving "minimum resources") and cheaper to (de)serialize than JSON. A Schema Registry gives you compile-time-ish safety and controlled schema evolution across the Go producer and the Kotlin consumer.

```json
// schemas/avro/event-raw.avsc
{
  "type": "record",
  "name": "RawEvent",
  "namespace": "com.platform.events",
  "fields": [
    { "name": "event_id", "type": "string" },
    { "name": "event_type", "type": "string" },
    { "name": "occurred_at", "type": "long", "logicalType": "timestamp-millis" },
    { "name": "payload", "type": "string" }
  ]
}
```

---

## 7. Ingestion Service (Go) — Complete Code

**Kafka client choice**: `franz-go`, not `confluent-kafka-go`. This isn't a style preference — it's load-bearing for "minimum resources": `confluent-kafka-go` wraps librdkafka via **cgo**, which means `CGO_ENABLED=0` (needed for a truly static, distroless binary) isn't an option. `franz-go` is pure Go, no C dependency, and its own published benchmarks show it's faster and more memory-efficient at both producing and consuming than the cgo-based client. Pure Go + faster + smaller image, for free.

```go
// services/ingestion-api/internal/ingest/event.go
package ingest

import (
	"errors"
	"time"
)

type RawEvent struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	OccurredAt int64  `json:"occurred_at"`
	Payload    string `json:"payload"`
}

func (e *RawEvent) Validate() error {
	// Envelope-level only: the fixed fields every event must have, regardless of
	// event_type. Per-type payload rules are data (eventtypes.EventTypeConfig, below),
	// not more cases added to this function.
	if e.EventID == "" {
		return errors.New("event_id is required")
	}
	if e.EventType == "" {
		return errors.New("event_type is required")
	}
	if e.OccurredAt == 0 {
		e.OccurredAt = time.Now().UnixMilli()
	}
	return nil
}
```

**The envelope (`RawEvent`) stays a fixed, typed struct on purpose** — that's what keeps Avro encoding fast and simple. What becomes data-driven is everything *about* a given `event_type`: which topic it routes to, what its `payload` JSON must contain, and whether it needs custom processing beyond declarative field rules. That lives in one small package:

```go
// services/ingestion-api/internal/eventtypes/config.go
package eventtypes

// FieldRule is deliberately narrow — presence and length checks cover the large
// majority of real validation needs. Anything more exotic (cross-field checks,
// external lookups) is exactly what CustomProcessor (below) is for — the registry
// escape hatch, not a reason to grow this struct into a mini rules-engine.
type FieldRule struct {
	Field     string `yaml:"field"`
	Required  bool   `yaml:"required"`
	MaxLength int    `yaml:"max_length"`
}

type EventTypeConfig struct {
	Name            string      `yaml:"name"`
	Topic           string      `yaml:"topic"`              // routing-as-data
	PayloadRules    []FieldRule `yaml:"payload_validation"` // empty slice = skip parsing entirely, zero cost
	CustomProcessor string      `yaml:"custom_processor"`   // optional — name of a registered function
}

type Config struct {
	EventTypes []EventTypeConfig `yaml:"event_types"`
}
```

```go
// services/ingestion-api/internal/eventtypes/registry.go
package eventtypes

import "fmt"

// Same Map + register() + get-or-fail-loudly shape used at every layer of this
// design, in every language — see §3.
var registry = map[string]EventTypeConfig{}

// LoadFromConfig fails fast: bad config refuses to start the process, rather
// than serving traffic with a silently broken event type.
func LoadFromConfig(cfg Config) error {
	for _, et := range cfg.EventTypes {
		if et.Name == "" {
			return fmt.Errorf("event type entry missing name")
		}
		if et.Topic == "" {
			return fmt.Errorf("event type %q missing topic", et.Name)
		}
		registry[et.Name] = et
	}
	return nil
}

func Get(name string) (EventTypeConfig, bool) {
	cfg, ok := registry[name]
	return cfg, ok
}
```

```go
// services/ingestion-api/internal/eventtypes/loader.go
package eventtypes

import (
	"os"

	"gopkg.in/yaml.v3"
)

func LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}
	return LoadFromConfig(cfg)
}
```

```go
// services/ingestion-api/internal/eventtypes/validate.go
package eventtypes

import (
	"encoding/json"
	"fmt"
)

// ValidatePayload only parses the payload JSON if the event type actually
// declares rules. An event type with no rules (e.g. a heartbeat/ping) pays
// zero parsing cost — "opt into validation," not "pay for it whether you use it or not."
func ValidatePayload(cfg EventTypeConfig, payloadJSON string) error {
	if len(cfg.PayloadRules) == 0 {
		return nil
	}
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &fields); err != nil {
		return fmt.Errorf("payload is not valid JSON: %w", err)
	}
	for _, rule := range cfg.PayloadRules {
		v, present := fields[rule.Field]
		if rule.Required && !present {
			return fmt.Errorf("payload.%s is required", rule.Field)
		}
		if s, ok := v.(string); ok && rule.MaxLength > 0 && len(s) > rule.MaxLength {
			return fmt.Errorf("payload.%s exceeds max length %d", rule.Field, rule.MaxLength)
		}
	}
	return nil
}
```

```go
// services/ingestion-api/internal/eventtypes/custom_processors.go
package eventtypes

// CustomProcessor is the escape hatch for logic that doesn't fit the declarative
// FieldRule shape — cross-field checks, enrichment, external lookups. Registered
// once per event type that genuinely needs it; every other event type, and all of
// routing/dedup/produce/tracing, stays fully generic and never duplicated.
type CustomProcessor func(payloadJSON string) (string, error)

var customProcessors = map[string]CustomProcessor{}

func RegisterCustomProcessor(name string, fn CustomProcessor) {
	customProcessors[name] = fn
}

func GetCustomProcessor(name string) (CustomProcessor, bool) {
	if name == "" {
		return nil, false
	}
	fn, ok := customProcessors[name]
	return fn, ok
}
```

```yaml
# config/event-types.yaml — the actual leverage point: this file is most of
# what "adding an event type" means from here on
event_types:
  - name: heartbeat
    topic: events.raw
    # no payload_validation -> fastest possible path, envelope checks only

  - name: page_view
    topic: events.raw
    payload_validation:
      - field: url
        required: true
        max_length: 2048

  - name: purchase
    topic: events.raw
    payload_validation:
      - field: amount_cents
        required: true
      - field: currency
        required: true
    custom_processor: purchaseEnrichment   # registered once in main.go, §below
```

Three event types, three genuinely different validation postures (none, declarative, declarative-plus-custom) — zero duplicated wiring, zero new HTTP handlers, zero new Kafka-producer code.

```go
// services/ingestion-api/internal/ingest/dedup.go
package ingest

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Deduper interface {
	SeenBefore(ctx context.Context, eventID string) (bool, error)
}

type redisDeduper struct{ client *redis.Client }

func NewRedisDeduper(addr string) Deduper {
	return &redisDeduper{client: redis.NewClient(&redis.Options{Addr: addr})}
}

func (d *redisDeduper) SeenBefore(ctx context.Context, eventID string) (bool, error) {
	// SETNX: atomic set-if-not-exists in one round trip; TTL bounds memory use
	setByUs, err := d.client.SetNX(ctx, "dedup:"+eventID, 1, 24*time.Hour).Result()
	if err != nil {
		return false, err
	}
	return !setByUs, nil // false from SetNX means the key already existed — we've seen it
}
```

```go
// services/ingestion-api/internal/ingest/avro.go
package ingest

import (
	"bytes"
	"encoding/binary"

	"github.com/hamba/avro/v2"
)

// Confluent wire format — stable, documented: [0x0][4-byte big-endian schema ID][Avro payload].
// The schema ID is fetched once from the Schema Registry at startup and cached; verify the
// exact registry-client call against current docs, this wire format itself won't change.
var rawEventSchema = avro.MustParse(rawEventSchemaJSON)

func encodeAvro(evt RawEvent, schemaID uint32) ([]byte, error) {
	payload, err := avro.Marshal(rawEventSchema, evt)
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(0x0)
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, schemaID)
	buf.Write(idBytes)
	buf.Write(payload)
	return buf.Bytes(), nil
}
```

```go
// services/ingestion-api/internal/ingest/producer.go
package ingest

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Verify exact functional-option names against current franz-go docs before shipping —
// this library's surface evolves; the config *intent* below is what matters:
// idempotent + acked writes, batched via a short linger window, compressed on the wire.
//
// Topic is a per-call argument, not a field fixed at construction: franz-go records
// already carry their topic individually, so routing-as-data (§eventtypes.Config.Topic)
// costs nothing extra — it's a variable where a constant used to be, same instruction count.
type Producer interface {
	Produce(ctx context.Context, topic string, evt RawEvent) error
	Close()
}

type kafkaProducer struct {
	client   *kgo.Client
	schemaID uint32
}

func NewKafkaProducer(brokers []string, schemaID uint32) (Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()), // smaller on the wire and on broker disk
		kgo.ProducerLinger(8*time.Millisecond),               // batches concurrent goroutines into fewer round trips
		kgo.RequiredAcks(kgo.AllISRAcks()),                    // durability — don't lose events on a broker failure
	)
	if err != nil {
		return nil, err
	}
	return &tracedProducer{inner: &kafkaProducer{client: client, schemaID: schemaID}}, nil
}

func (p *kafkaProducer) Produce(ctx context.Context, topic string, evt RawEvent) error {
	avroBytes, err := encodeAvro(evt, p.schemaID)
	if err != nil {
		return err
	}
	record := &kgo.Record{Topic: topic, Key: []byte(evt.EventID), Value: avroBytes}
	// blocks only this goroutine, not the server — cheap, since goroutines are ~2KB each
	res := p.client.ProduceSync(ctx, record)
	return res.FirstErr()
}

func (p *kafkaProducer) Close() { p.client.Close() }

// Same decorator shape used throughout this design (retry/cache/circuit-breaker/tracing
// wrappers in prior work) — one wrapper, applied once, at construction time.
type tracedProducer struct{ inner *kafkaProducer }

func (t *tracedProducer) Produce(ctx context.Context, topic string, evt RawEvent) error {
	_, span := otel.Tracer("ingestion-api").Start(ctx, "kafka.produce")
	span.SetAttributes(attribute.String("kafka.topic", topic))
	defer span.End()
	return t.inner.Produce(ctx, topic, evt)
}
func (t *tracedProducer) Close() { t.inner.Close() }
```

```go
// services/ingestion-api/internal/httpapi/handler.go
package httpapi

import (
	"encoding/json"
	"net/http"

	"event-platform/ingestion-api/internal/eventtypes"
	"event-platform/ingestion-api/internal/ingest"
)

type Handler struct {
	producer ingest.Producer
	deduper  ingest.Deduper
}

func NewHandler(p ingest.Producer, d ingest.Deduper) *Handler {
	return &Handler{producer: p, deduper: d}
}

// One handler. Every event type in event-types.yaml, present and future,
// flows through this same function — adding event type #501 never touches this file.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var evt ingest.RawEvent
	if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if err := evt.Validate(); err != nil { // envelope-level only
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	cfg, ok := eventtypes.Get(evt.EventType)
	if !ok {
		// Fail closed, not open: an unregistered event type is a config gap to fix,
		// not traffic to silently accept into an unvalidated default bucket.
		http.Error(w, "unregistered event_type — add it to event-types.yaml", http.StatusUnprocessableEntity)
		return
	}

	if err := eventtypes.ValidatePayload(cfg, evt.Payload); err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	if proc, ok := eventtypes.GetCustomProcessor(cfg.CustomProcessor); ok {
		enriched, err := proc(evt.Payload)
		if err != nil {
			http.Error(w, "processing failed: "+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		evt.Payload = enriched
	}

	seen, err := h.deduper.SeenBefore(r.Context(), evt.EventID)
	if err != nil {
		http.Error(w, "dedup check failed", http.StatusServiceUnavailable)
		return
	}
	if seen {
		w.WriteHeader(http.StatusOK) // idempotent: client's retry looks like success
		return
	}

	if err := h.producer.Produce(r.Context(), cfg.Topic, evt); err != nil {
		http.Error(w, "ingest failed", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
```

```go
// services/ingestion-api/internal/httpapi/middleware.go
package httpapi

import (
	"net/http"

	"go.opentelemetry.io/otel"
)

func WithTracing(next http.Handler) http.Handler {
	tracer := otel.Tracer("ingestion-api")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "http.ingest")
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// WithRateLimit: a semaphore caps in-flight requests, shedding load past capacity
// instead of letting unbounded goroutines exhaust memory — "minimum resources"
// means bounded resources, not just small ones.
func WithRateLimit(next http.Handler, maxConcurrent int) http.Handler {
	sem := make(chan struct{}, maxConcurrent)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			next.ServeHTTP(w, r)
		default:
			http.Error(w, "at capacity, retry with backoff", http.StatusServiceUnavailable)
		}
	})
}
```

```go
// services/ingestion-api/internal/config/config.go
package config

import "os"

type Config struct {
	ListenAddr     string
	KafkaBrokers   []string
	RedisAddr      string
	OTLPEndpoint   string
	SchemaID       uint32
	EventTypesPath string // path to event-types.yaml — the leverage point from §3
	MaxConcurrent  int
}

func Load() Config {
	return Config{
		ListenAddr:     getEnv("LISTEN_ADDR", ":8080"),
		KafkaBrokers:   []string{getEnv("KAFKA_BROKERS", "kafka:9092")},
		RedisAddr:      getEnv("REDIS_ADDR", "redis:6379"),
		OTLPEndpoint:   getEnv("OTLP_ENDPOINT", "otel-collector:4317"),
		EventTypesPath: getEnv("EVENT_TYPES_PATH", "/etc/config/event-types.yaml"),
		MaxConcurrent:  2000,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

```go
// services/ingestion-api/cmd/server/main.go
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"event-platform/ingestion-api/internal/config"
	"event-platform/ingestion-api/internal/customprocessors"
	"event-platform/ingestion-api/internal/eventtypes"
	"event-platform/ingestion-api/internal/httpapi"
	"event-platform/ingestion-api/internal/ingest"
	"event-platform/ingestion-api/internal/observability"
)

func main() {
	cfg := config.Load()
	shutdownTracing := observability.InitTracing(cfg.OTLPEndpoint)
	defer shutdownTracing(context.Background())

	// Fail fast: a broken event-types.yaml crashes the pod on boot, not on
	// request #40,000 — see §3.
	if err := eventtypes.LoadFromFile(cfg.EventTypesPath); err != nil {
		log.Fatalf("event type config invalid, refusing to start: %v", err)
	}
	// Registered once per event type that needs the escape hatch — everything
	// else in event-types.yaml needs zero lines of Go.
	eventtypes.RegisterCustomProcessor("purchaseEnrichment", customprocessors.PurchaseEnrichment)

	producer, err := ingest.NewKafkaProducer(cfg.KafkaBrokers, cfg.SchemaID)
	if err != nil {
		log.Fatalf("kafka producer init: %v", err)
	}
	defer producer.Close()
	deduper := ingest.NewRedisDeduper(cfg.RedisAddr)

	mux := http.NewServeMux()
	handler := httpapi.NewHandler(producer, deduper)
	mux.Handle("/v1/events", httpapi.WithTracing(httpapi.WithRateLimit(handler, cfg.MaxConcurrent)))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{
		Addr: cfg.ListenAddr, Handler: mux,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second,
	}

	go func() {
		log.Printf("listening on %s", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx) // stop accepting new connections; let in-flight requests finish
}
```

```go
// services/ingestion-api/internal/customprocessors/purchase.go — the one file that
// exists because "purchase" needs more than declarative field rules. Every future
// event type that DOESN'T need this stays entirely in event-types.yaml.
package customprocessors

import "encoding/json"

func PurchaseEnrichment(payloadJSON string) (string, error) {
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &fields); err != nil {
		return "", err
	}
	// example: normalize currency to uppercase ISO-4217 before it reaches Kafka
	if c, ok := fields["currency"].(string); ok {
		fields["currency"] = toUpper(c)
	}
	out, err := json.Marshal(fields)
	return string(out), err
}

func toUpper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}
```

```dockerfile
# services/ingestion-api/Dockerfile — CGO_ENABLED=0 only works because franz-go is pure Go
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ingestion-api ./cmd/server

FROM gcr.io/distroless/static-debian12
COPY --from=build /ingestion-api /ingestion-api
EXPOSE 8080
ENTRYPOINT ["/ingestion-api"]
```

---

## 8. Kafka — Topic & Partition Sizing

`events.raw`, **12 partitions**, reasoned rather than guessed: at 5,000 req/s peak, each partition needs to sustain only ~417 msg/s — trivial for a single partition. Twelve gives comfortable headroom, lets the Kafka Streams app run up to 12 parallel `StreamThreads` (matching typical small-cluster core counts), and avoids the opposite failure mode: over-partitioning inflates open file handles and per-partition replication overhead for no benefit at this volume. Six to twelve is the right range here — not sixty.

---

## 9. Kafka Connect — Sink to Postgres

**Gotcha worth flagging up front**: `debezium/connect` ships Debezium's *source* connectors pre-installed, but the JDBC *sink* connector used below is a separate Confluent plugin — it isn't in that image by default. Extend it:

```dockerfile
# infra/kafka/connect.Dockerfile
FROM debezium/connect:latest
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4
```

```json
// infra/kafka/connectors/postgres-raw-sink.json
{
  "name": "postgres-raw-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/app",
    "connection.user": "app",
    "connection.password": "app",
    "topics": "events.raw",
    "table.name.format": "raw_events",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "event_id",
    "auto.create": "true",
    "auto.evolve": "true",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

`"insert.mode": "upsert"` (not `"insert"`) is deliberate: it makes redelivery of the same `event_id` — which Kafka's at-least-once delivery guarantees will eventually do — a safe overwrite instead of a primary-key violation or a duplicate row.

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @infra/kafka/connectors/postgres-raw-sink.json \
  http://localhost:8083/connectors
```

---

## 10. Kafka Streams (Kotlin) — Complete Code

**Design note before the code**: the generic step chain below only ever transforms `RawEvent -> RawEvent` (dedup, filtering, in-place enrichment). Aggregation (windowed count) is deliberately handled as structured config *outside* that chain, not as another chainable step — because aggregation changes the value type (`RawEvent` in, a count out), and Kotlin/Java generics make a fully arbitrary "any step can change the type to anything" chain a genuinely hard problem to do safely, not a five-minute abstraction. This is the same "terminal operation, not a chain link" reasoning applied to `groupBy` in the list-transform layer of prior work — reused here because it's correct here too, not because it's familiar.

```kotlin
// services/stream-processor/src/main/kotlin/com/platform/streams/topology/processors/ProcessorRegistry.kt
package com.platform.streams.topology.processors

import org.apache.kafka.streams.kstream.TransformerSupplier

// The exact same shape as the field-renderer / async-checker / workflow-step
// registries from prior architecture work: a Map plus a register function,
// so adding a processing step never touches TopologyBuilder itself.
object ProcessorRegistry {
    private val registry = mutableMapOf<String, TransformerSupplier<String, Any, org.apache.kafka.streams.KeyValue<String, Any>>>()

    fun register(name: String, supplier: TransformerSupplier<String, Any, org.apache.kafka.streams.KeyValue<String, Any>>) {
        registry[name] = supplier
    }

    fun get(name: String) = registry[name]
        ?: throw IllegalStateException("No processor registered for \"$name\"")
}
```

```kotlin
// services/stream-processor/src/main/kotlin/com/platform/streams/topology/TopologyDefinition.kt
package com.platform.streams.topology

// THE data. A new aggregation with a different window or group-by field is a
// new value of this class — zero new Kotlin. A new STEP TYPE (fundamentally new
// processing, not a new instance of existing processing) is what requires code,
// in BuiltinSteps.kt, once.
data class TopologyStep(
    val id: String,
    val type: String,                    // must match a name registered in StepRegistry
    val config: Map<String, Any> = emptyMap()
)

data class TopologyDefinition(
    val sourceTopic: String,
    val steps: List<TopologyStep>,       // generic RawEvent -> RawEvent chain: dedup, filters, ...
    val groupByField: String = "eventType",
    val windowMinutes: Long = 1,
    val sinkTopic: String
)
```

```kotlin
// services/stream-processor/src/main/kotlin/com/platform/streams/serde/AvroSerdes.kt
package com.platform.streams.serde

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Grouped

// RawEvent here is generated by the Gradle Avro plugin from schemas/avro/event-raw.avsc
// at build time (standard practice — hand-writing SpecificRecord implementations by
// hand is what that plugin exists to avoid). This class shows the shape expected of it.
data class RawEvent(val eventId: String, val eventType: String, val occurredAt: Long, val payload: String)

class AvroSerdes(private val schemaRegistryUrl: String) {
    private val config = mapOf("schema.registry.url" to schemaRegistryUrl)

    fun stringSerde() = Serdes.String()
    fun longSerde() = Serdes.Long()

    fun rawEventSerde(): SpecificAvroSerde<RawEvent> =
        SpecificAvroSerde<RawEvent>().apply { configure(config, false) }

    fun groupedByType() = Grouped.with(stringSerde(), stringSerde())
}
```

```kotlin
// services/stream-processor/src/main/kotlin/com/platform/streams/topology/StepRegistry.kt
package com.platform.streams.topology

import org.apache.kafka.streams.kstream.KStream
import com.platform.streams.serde.RawEvent

// Same Map + register() + get-or-fail-loudly shape as eventtypes.Registry on the
// Go side, and as ProcessorRegistry above — one seam shape, every layer, every language.
typealias StreamStepFn = (KStream<String, RawEvent>, Map<String, Any>) -> KStream<String, RawEvent>

object StepRegistry {
    private val steps = mutableMapOf<String, StreamStepFn>()

    fun register(type: String, fn: StreamStepFn) { steps[type] = fn }

    fun get(type: String): StreamStepFn = steps[type]
        ?: throw IllegalStateException("No topology step registered for type \"$type\"")
}
```

```kotlin
// services/stream-processor/src/main/kotlin/com/platform/streams/topology/BuiltinSteps.kt
package com.platform.streams.topology

import com.platform.streams.topology.processors.ProcessorRegistry

// Registered once. This file only grows when a genuinely NEW kind of RawEvent
// transformation is needed — a new AGGREGATION using an EXISTING kind belongs
// in TopologyDefinition (data), not here.
fun registerBuiltinSteps() {
    StepRegistry.register("dedup") { stream, _ ->
        // belt-and-suspenders: the Redis SETNX at the edge (§7) already dedups
        // once; this catches anything that slipped through under at-least-once redelivery
        stream.transform(ProcessorRegistry.get("dedup"))
    }

    StepRegistry.register("filterByType") { stream, config ->
        @Suppress("UNCHECKED_CAST")
        val allowed = config["allowedTypes"] as? List<String> ?: emptyList()
        stream.filter { _, v -> allowed.isEmpty() || v.eventType in allowed }
    }
}
```

```kotlin
// services/stream-processor/src/main/kotlin/com/platform/streams/topology/TopologyBuilder.kt
package com.platform.streams.topology

import com.platform.streams.serde.AvroSerdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import java.time.Duration

// Built ONCE, at JVM startup — walking `definition.steps` costs microseconds
// and happens a single time. The Kafka Streams processor graph that results
// executes identically to one that was hand-written line by line; Kafka Streams
// has no idea, and pays no per-record cost, for how its graph was assembled.
// This is what makes "data-driven" free here — see §3.
class TopologyBuilder(private val definition: TopologyDefinition, private val serdes: AvroSerdes) {
    fun build(): Topology {
        val builder = StreamsBuilder()
        var stream = builder.stream(definition.sourceTopic, Consumed.with(serdes.stringSerde(), serdes.rawEventSerde()))

        // generic middle: RawEvent -> RawEvent, entirely driven by data
        for (step in definition.steps) {
            stream = StepRegistry.get(step.type)(stream, step.config)
        }

        // terminal stage: aggregation changes the value type, so it's structured
        // config fields on TopologyDefinition, not a chained step — see the design
        // note at the top of this section
        stream
            .groupBy({ _, v -> extractField(v, definition.groupByField) }, serdes.groupedByType())
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(definition.windowMinutes)))
            .count()
            .toStream()
            .map { windowedKey, count -> org.apache.kafka.streams.KeyValue(windowedKey.key(), count) }
            .to(definition.sinkTopic, Produced.with(serdes.stringSerde(), serdes.longSerde()))

        return builder.build()
    }
}

private fun extractField(evt: com.platform.streams.serde.RawEvent, field: String): String =
    if (field == "eventType") evt.eventType else evt.eventType // extend for real multi-field grouping as needed
```

```kotlin
// services/stream-processor/src/main/kotlin/com/platform/streams/Application.kt
package com.platform.streams

import com.platform.streams.topology.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import java.time.Duration
import java.util.Properties

fun main() {
    registerBuiltinSteps() // one-time registration of step TYPES — code, changes rarely

    // THE topology SHAPE — data, changes often. Load this from YAML/a ConfigMap in
    // production (same "fail fast at startup" discipline as event-types.yaml, §7);
    // inlined here for readability.
    val definition = TopologyDefinition(
        sourceTopic = "events.raw",
        steps = listOf(
            TopologyStep(id = "dedup", type = "dedup"),
        ),
        groupByField = "eventType",
        windowMinutes = 1,
        sinkTopic = "events.enriched",
    )

    val props = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-processor")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BROKERS") ?: "kafka:9092")
        put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4)        // matched to partition count (§8) / available cores
        put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)   // unchanged by this refactor — see §3
        put(StreamsConfig.DEFAULT_REPLICATION_FACTOR_CONFIG, 3)
    }

    val serdes = com.platform.streams.serde.AvroSerdes(System.getenv("SCHEMA_REGISTRY_URL") ?: "http://schema-registry:8081")
    val streams = KafkaStreams(TopologyBuilder(definition, serdes).build(), props)
    Runtime.getRuntime().addShutdownHook(Thread { streams.close(Duration.ofSeconds(10)) })
    streams.start()
}
```

**What adding a second aggregation now costs**: a new `TopologyDefinition` (or a second `KafkaStreams` instance running alongside this one, for a genuinely independent aggregation) — a data-structure literal, not a new hand-wired `StreamsBuilder` graph. What adding a genuinely new step TYPE costs: one function in `BuiltinSteps.kt`, once, ever, for that type.

`EXACTLY_ONCE_V2` covers Kafka-to-Kafka processing (`events.raw` → `events.enriched`) — it does not extend to the JDBC sink at the end of the pipeline, which is why §9's connector uses `upsert` rather than relying on Streams' exactly-once guarantee to carry all the way to Postgres. Nothing about this section's redesign touches that guarantee — it's configured in the same `props` block, untouched.

---

## 11. Redis & Postgres — Scope

**Redis**: exactly one job here — the `SETNX`-based dedup check in §7. Nothing else; no pub/sub, no session state, no cache for this pipeline. A single small Redis instance easily handles a `SETNX` + TTL per request at this volume.

**Postgres**: sink target only, written to by Kafka Connect, never touched directly by the ingestion path. Two tables:

```sql
CREATE TABLE raw_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE enriched_counts (
    event_type TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (event_type, window_start)
);
```

---

## 12. Observability

Same shape as standard OTel practice: every service exports OTLP to a Collector, which routes to Tempo (traces) and Prometheus (metrics), visualized in Grafana. Condensed here since the pattern itself isn't new — what matters for *this* system is what you watch:

- **Request rate** (should hold steady near 1,667/s for the full 10 minutes of the load test in §13)
- **Ingestion-api CPU/memory** — this is your literal "minimum resources" proof; watch it live during the load test, not after
- **Kafka consumer lag** on `stream-processor`'s consumer group — should stay near zero; a growing lag means the streams tier can't keep up with the ingestion tier
- **End-to-end latency**, ingestion → `events.enriched`, via the trace waterfall

```yaml
# infra/otel-collector/otel-collector-config.yaml (condensed — same shape as standard OTel practice)
receivers:
  otlp: { protocols: { grpc: { endpoint: 0.0.0.0:4317 }, http: { endpoint: 0.0.0.0:4318 } } }
processors:
  batch: { timeout: 5s }
  memory_limiter: { check_interval: 1s, limit_mib: 512 }
exporters:
  otlphttp/tempo: { endpoint: http://tempo:4318 }
  prometheusremotewrite: { endpoint: http://prometheus:9090/api/v1/write }
service:
  pipelines:
    traces: { receivers: [otlp], processors: [memory_limiter, batch], exporters: [otlphttp/tempo] }
    metrics: { receivers: [otlp], processors: [memory_limiter, batch], exporters: [prometheusremotewrite] }
```

At this request volume, tail-sampling (needed at "billions of requests" scale) isn't necessary — sample 100% and use the plain `otel/opentelemetry-collector` image; you only need `-contrib` if you later add tail-sampling.

---

## 13. Load Testing — Proving the Target

`constant-arrival-rate` is the deliberate choice here, not `ramping-vus` — it holds a **fixed rate** regardless of response time, which is exactly what "1,667 req/s for 10 minutes" requires you to prove, rather than just "as many requests as the system can absorb."

```javascript
// loadtest/ingestion_burst.js
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 1667,
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 200,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<200'],
  },
};

export default function () {
  const payload = JSON.stringify({
    event_id: `${__VU}-${__ITER}-${Date.now()}`,
    event_type: 'page_view',
    occurred_at: Date.now(),
    payload: '{"path":"/home"}',
  });
  const res = http.post('http://ingestion-api:8080/v1/events', payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 202': (r) => r.status === 202 });
}
```

Run it watching §12's dashboards live. The output you actually want isn't "did it pass" — it's the observed p95 CPU/memory per replica at target rate, which is what feeds §15's resource sizing.

---

## 14. Full `docker-compose.yml`

```yaml
# infra/docker-compose.yml — no top-level `version:` key; the Compose Specification
# deprecated it and modern `docker compose` ignores it with a warning.
name: event-platform

networks:
  backbone: {}

services:
  postgres:
    image: postgres:17
    environment: { POSTGRES_DB: app, POSTGRES_USER: app, POSTGRES_PASSWORD: app }
    ports: ["5432:5432"]
    healthcheck: { test: ["CMD-SHELL", "pg_isready -U app"], interval: 5s, retries: 10 }
    networks: [backbone]

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    healthcheck: { test: ["CMD", "redis-cli", "ping"], interval: 5s, retries: 10 }
    networks: [backbone]

  kafka:
    image: apache/kafka:latest    # KRaft by default — Kafka 4.0 removed ZooKeeper entirely
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports: ["9092:9092"]
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"]
      interval: 10s
      retries: 10
    networks: [backbone]

  schema-registry:
    image: confluentinc/cp-schema-registry:latest
    depends_on: { kafka: { condition: service_healthy } }
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
    ports: ["8081:8081"]
    networks: [backbone]

  connect:
    build: { context: ./kafka, dockerfile: connect.Dockerfile }   # §9's extended image with the JDBC sink plugin
    depends_on:
      kafka: { condition: service_healthy }
      postgres: { condition: service_healthy }
      schema-registry: { condition: service_started }
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: connect-cluster
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
    ports: ["8083:8083"]
    networks: [backbone]

  ingestion-api:
    build: { context: ../services/ingestion-api }
    depends_on:
      kafka: { condition: service_healthy }
      redis: { condition: service_healthy }
    environment:
      KAFKA_BROKERS: "kafka:9092"
      REDIS_ADDR: "redis:6379"
      OTLP_ENDPOINT: "otel-collector:4317"
      EVENT_TYPES_PATH: "/etc/config/event-types.yaml"
    volumes: ["../config/event-types.yaml:/etc/config/event-types.yaml:ro"]
    ports: ["8080:8080"]
    deploy: { replicas: 2, resources: { limits: { cpus: "0.75", memory: "256M" } } }  # start here, adjust from §13's measurements
    networks: [backbone]

  stream-processor:
    build: { context: ../services/stream-processor }
    depends_on: { kafka: { condition: service_healthy } }
    environment: { KAFKA_BROKERS: "kafka:9092", SCHEMA_REGISTRY_URL: "http://schema-registry:8081" }
    networks: [backbone]

  tempo:
    image: grafana/tempo:latest
    command: ["-config.file=/etc/tempo.yaml"]
    volumes: ["./tempo/tempo.yaml:/etc/tempo.yaml:ro"]
    networks: [backbone]

  otel-collector:
    image: otel/opentelemetry-collector:latest   # plain core image is enough — see §12
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes: ["./otel-collector/otel-collector-config.yaml:/etc/otel-collector-config.yaml:ro"]
    ports: ["4317:4317", "4318:4318"]
    depends_on: [tempo]
    networks: [backbone]

  prometheus:
    image: prom/prometheus:latest
    command: ["--config.file=/etc/prometheus/prometheus.yml", "--web.enable-remote-write-receiver"]
    volumes: ["./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro"]
    ports: ["9090:9090"]
    networks: [backbone]

  grafana:
    image: grafana/grafana:latest
    volumes: ["./grafana/provisioning:/etc/grafana/provisioning:ro"]
    ports: ["3001:3000"]
    depends_on: [prometheus, tempo]
    networks: [backbone]
```

---

## 15. Kubernetes — Sizing From Measurement, Not Guesses

```yaml
# infra/k8s/ingestion-api/deployment.yaml — resource values below are illustrative;
# replace with YOUR §13 load-test observations, not copied verbatim
apiVersion: apps/v1
kind: Deployment
metadata: { name: ingestion-api }
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: ingestion-api
          image: event-platform/ingestion-api:latest
          resources:
            requests: { cpu: "300m", memory: "128Mi" }
            limits: { cpu: "750m", memory: "256Mi" }
          readinessProbe: { httpGet: { path: /healthz, port: 8080 }, periodSeconds: 5 }
          livenessProbe: { httpGet: { path: /healthz, port: 8080 }, periodSeconds: 10 }
```

```yaml
# infra/k8s/ingestion-api/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: { name: ingestion-api }
spec:
  scaleTargetRef: { apiVersion: apps/v1, kind: Deployment, name: ingestion-api }
  minReplicas: 2    # floor for availability — one instance alone can plausibly carry the whole target (§1)
  maxReplicas: 6    # ceiling sized from §13's load test, not guessed
  metrics:
    - type: Resource
      resource: { name: cpu, target: { type: Utilization, averageUtilization: 65 } }
```

`minReplicas: 2` is for availability (a deploy or a crash shouldn't take the whole ingestion path down), not because one instance is insufficient for throughput — per §1's math, it very well might be.

---

## 16. Scope Boundaries

- **This is a throughput/language/streaming architecture, not a governance one.** If you need the RBAC, audit logging, compliance primitives, or multi-tenant isolation covered in prior architecture work, layer those in explicitly — they weren't re-derived here to stay scoped to what was actually asked.
- **Exactly-once stops at Kafka.** `EXACTLY_ONCE_V2` (§10) guarantees Kafka-to-Kafka; the Postgres sink (§9) is at-least-once made *safe* via `upsert`, which is a different (weaker, but sufficient) guarantee than true exactly-once delivery to Postgres.
- **This isn't a durable-workflow engine.** If "process 1M events" later grows into "run a multi-step business process per event with human approval steps," that's the Temporal-based durable execution pattern from prior work, not this pipeline.
- **1,667 req/s is not "billions of users" scale.** Don't over-provision this against a different problem than the one stated — re-read §1 before adding infrastructure "just in case."

---

## 17. References

- franz-go (pure-Go Kafka client, §7): https://github.com/twmb/franz-go
- go-redis: https://github.com/redis/go-redis
- hamba/avro (pure-Go Avro, §7): https://github.com/hamba/avro
- Apache Kafka: https://github.com/apache/kafka
- Kafka Connect JDBC connector: https://github.com/confluentinc/kafka-connect-jdbc
- Kafka Streams (part of Apache Kafka, §10): https://github.com/apache/kafka/tree/trunk/streams
- k6 (§13): https://github.com/grafana/k6
- OpenTelemetry Collector: https://github.com/open-telemetry/opentelemetry-collector
- Prometheus: https://github.com/prometheus/prometheus
- Grafana Tempo: https://github.com/grafana/tempo
