# Design Decisions, Critical Thinking, Debugging & Config Reference

Companion to `high-throughput-ingestion-architecture.md`. Sections 1-9 cover the foundation (the actual built system, its language/config/debugging reference). Sections 10-17 extend the same decision-tree/pseudocode discipline to billion-user scale — multi-region topology, sharding, distributed algorithms, and the failure modes that only appear once a single cluster and a single database are no longer enough. Style rule followed throughout, verified by script rather than by eye: **zero comments inside any code block** — every "why" lives in the tree or prose around the code, never inside it.

---

## 1. Design Decision Trees

Format: branch (the real alternatives) → root cause (the sequence of facts that forced the question) → trade-offs (why this specific answer, stated against the alternatives, not in isolation).

```
DECISION: Ingestion tier language
│
├── branch: Go / Rust / Node.js / Python / Java
│
├── root cause
│   └── sequence flow:
│         1,000,000 req / 600s -> 1,667 req/s sustained
│         -> work per request is mostly I/O-bound (parse, dedup check, produce)
│         -> concurrency primitive cost dominates, not raw compute
│         -> thousands of simultaneous in-flight requests must be cheap
│
└── trade-offs
    └── why Go, specifically:
          goroutine ~2KB vs OS thread ~2-8MB -> thousands of in-flight
          requests cost megabytes, not gigabytes
          static binary -> distroless image, no runtime dependency
          Rust: comparable or better raw efficiency, 3-5x the build time
          for a target Go clears with room to spare -> paying for
          complexity the target does not demand
          Node.js: single-threaded event loop needs explicit clustering
          to use more than one core
          Python: GIL caps true parallelism per process -> needs several
          times more replicas for equal throughput
```

```
DECISION: Stream-processing tier language
│
├── branch: Kotlin/JVM / Go / Python / Rust
│
├── root cause
│   └── sequence flow:
│         need windowed aggregation, fault-tolerant state stores,
│         exactly-once semantics across partitions
│         -> Kafka Streams is the library that provides all three
│         -> Kafka Streams is JVM-only, no non-JVM equivalent exists
│
└── trade-offs
    └── why Kotlin, specifically:
          Java gives the identical DSL/runtime; Kotlin is chosen for
          syntax only, nothing is gained or lost functionally
          hand-rolling the same guarantees in Go/Rust means
          reimplementing windowing, state-store fault tolerance, and
          exactly-once from scratch -> not a reasonable trade
```

```
DECISION: Serialization format for events on the wire
│
├── branch: Avro / Protobuf / JSON / MessagePack
│
├── root cause
│   └── sequence flow:
│         Kafka Connect needs a converter that understands the schema
│         -> Confluent's AvroConverter is the most trodden path in the
│            Kafka ecosystem specifically
│         -> Schema Registry gives compile-time-adjacent safety and
│            controlled evolution across independently-deployed
│            producer and consumer services
│
└── trade-offs
    └── why Avro over the alternatives:
          smaller than JSON on the wire, cheaper to deserialize
          Protobuf is comparable or faster, but its Kafka Connect
          converter path is less standard than Avro's
          MessagePack has no Schema Registry integration in this
          ecosystem, loses the evolution-safety property entirely
          JSON was the default that was actually shipped first and
          proven wrong at scale, see the CPU-bottleneck tree below
```

```
DECISION: Full JSON unmarshal vs targeted field extraction
│
├── branch: encoding/json.Unmarshal into map[string]interface{}
│           vs jsonparser.Get/Set targeted access
│
├── root cause
│   └── sequence flow:
│         payload assumed ~200-500 bytes at design time
│         -> full unmarshal cost judged negligible next to network I/O
│         -> real load test run at 500KB payload size
│         -> profile showed encoding/json.Unmarshal at 38.63% of
│            cumulative CPU, ingestion-api at 394% CPU for 58 req/s
│         -> root cause: full parse cost scales with document size,
│            not with the number of fields actually being checked
│
└── trade-offs
    └── why jsonparser specifically:
          cost becomes proportional to fields touched, not payload size
          isolated benchmark showed roughly 21x speedup on small
          payloads and a drop from 15 allocations to 1
          cost: raw []byte field access instead of a typed struct,
          loses some compile-time ergonomics in exchange for the
          allocation profile
          this is the single highest-leverage fix found in the entire
          system, see section 4
```

```
DECISION: Kafka topic partition count
│
├── branch: 6 / 12 / 60 / auto-scaled
│
├── root cause
│   └── sequence flow:
│         5,000 req/s peak design target
│         -> divided across partitions, each partition only needs to
│            sustain a few hundred msg/s
│         -> single partition throughput ceiling is far above that
│
└── trade-offs
    └── why 12, specifically:
          matches a realistic small-cluster core count, letting the
          Kafka Streams app run that many parallel StreamThreads
          more partitions -> more open file handles, more replication
          traffic, more per-partition memory, for zero benefit below
          the throughput that would actually need it
          fewer than 6 -> consumer parallelism ceiling becomes the
          bottleneck before the broker does
```

```
DECISION: Replication factor and min.insync.replicas
│
├── branch: RF=1/min-isr=1, RF=3/min-isr=1, RF=3/min-isr=2, RF=5/min-isr=3
│
├── root cause
│   └── sequence flow:
│         a broker WILL eventually fail or restart
│         -> question is whether that event loses data, halts writes
│            entirely, or degrades gracefully
│
└── trade-offs
    └── why RF=3, min.insync.replicas=2:
          RF=1 -> any broker loss is data loss, not just an outage
          RF=3/min-isr=1 -> a lone surviving replica can silently
          diverge from what was acknowledged as durable
          RF=3/min-isr=2 -> tolerates one broker failure with zero
          data loss; writes only halt if two of three are down
          RF=3/min-isr=3 (equal to RF) -> any single broker loss halts
          all writes, trading availability for a guarantee already met
          at min-isr=2
```

```
DECISION: Producer acknowledgment and idempotence
│
├── branch: acks=0 / acks=1 / acks=all, with or without
│           enable.idempotence
│
├── root cause
│   └── sequence flow:
│         a produce call can time out ambiguously: the broker may or
│         may not have actually received it
│         -> naive retry-on-timeout risks a duplicate
│         -> not retrying risks silent data loss
│
└── trade-offs
    └── why acks=all + enable.idempotence=true:
          acks=0 -> fire and forget, fastest, loses data on any broker
          hiccup with zero visibility
          acks=1 -> leader-only ack, data loss window if the leader
          dies before replication completes
          acks=all -> waits for every in-sync replica, the durability
          floor this design requires
          idempotence turns "retry on ambiguous timeout" from a risk
          into a broker-enforced no-op-if-already-received guarantee,
          at effectively zero added latency
```

```
DECISION: Consumer offset commit timing
│
├── branch: commit-before-processing / auto-commit-on-interval /
│           commit-after-successful-processing
│
├── root cause
│   └── sequence flow:
│         a consumer can crash between reading a record and finishing
│         work on it
│         -> the question is which side of that crash counts as "done"
│
└── trade-offs
    └── why commit-after-successful-processing:
          commit-before -> a crash mid-processing silently loses that
          record, it will never be redelivered
          auto-commit-on-interval -> same failure mode, just on a timer
          instead of per-record, slightly better throughput, same risk
          commit-after -> a crash mid-processing replays the record on
          restart; this is what "at-least-once" actually means, and it
          is the correct default absent a stronger reason to trade it
          for throughput
```

```
DECISION: Rebalance protocol
│
├── branch: eager (default historically) / cooperative-sticky
│
├── root cause
│   └── sequence flow:
│         every deploy, scale event, or crash triggers a rebalance
│         -> eager protocol revokes every partition from every
│            consumer before reassigning any of them
│         -> this pauses the entire consumer group, not just the
│            instance being replaced
│
└── trade-offs
    └── why cooperative-sticky:
          only moves the partitions that actually need to move
          unaffected consumers keep processing during the rebalance
          cost: a config value change, not new code -> effectively
          free relative to the operational payoff
```

```
DECISION: Deduplication location
│
├── branch: Redis SETNX at the edge / Kafka Streams stateful dedup /
│           database unique constraint only
│
├── root cause
│   └── sequence flow:
│         clients retry on ambiguous network failures
│         -> the same event_id can arrive more than once
│         -> deduping at the very first point of contact avoids
│            wasted work at every downstream stage
│
└── trade-offs
    └── why both Redis AND Kafka Streams dedup, deliberately redundant:
          Redis catches it before a single byte reaches Kafka
          Kafka Streams dedup is belt-and-suspenders against anything
          that slipped past Redis under a race condition
          database-unique-constraint-only -> catches it latest, after
          the most expensive work (parse, produce, consume) already ran
```

---

## 2. Concurrency vs Parallelism — the Distinction This System Actually Relies On

Concurrency is structure: many tasks in flight, interleaved. Parallelism is execution: multiple tasks actually running at the same instant. This system uses both, at two different layers, and conflating them is a common source of wrong intuition.

```
Go ingestion-api
    concurrency unit: goroutine, ~2KB each
    parallelism unit: GOMAXPROCS, defaults to logical CPU count
    -> thousands of goroutines can be IN FLIGHT (concurrency)
    -> only GOMAXPROCS of them are RUNNING at any instant (parallelism)
    -> the rest are cooperatively scheduled onto that fixed pool

Kafka Streams stream-processor
    concurrency unit: task, one per assigned partition
    parallelism unit: num.stream.threads
    -> a thread can only process the partitions assigned to it
    -> true parallelism ceiling is min(partition_count, num.stream.threads)
    -> setting num.stream.threads above partition count buys nothing,
       those threads sit idle
```

The wrong intuition this corrects: "more goroutines/threads always means more speed." It only does while the bottleneck is I/O wait. Once the bottleneck is CPU (the 500KB JSON incident), adding concurrency does nothing — the fixed number of cores is already saturated regardless of how many goroutines are queued behind them.

---

## 3. Optimization Playbook

### 3.1 Database level

```
PRACTICE: connection pooling with hard limits
impact if skipped: connection exhaustion under load, Postgres itself
                   becomes the outage rather than a symptom of one

PRACTICE: upsert instead of insert on the sink connector
impact if skipped: at-least-once delivery becomes duplicate rows or
                   primary-key violations on redelivery

PRACTICE: index only the columns actually queried by read paths
impact if over-applied: every additional index slows every write,
                   a cost paid on 100% of inserts to speed up some
                   fraction of reads

PRACTICE: batch inserts from the sink connector rather than
          row-by-row
impact if skipped: transaction overhead dominates over the actual
                   row-write cost at high volume

PRACTICE: let large JSONB payloads TOAST automatically
impact if fought (e.g. forcing everything inline): larger table
                   pages, worse cache hit rate on every query that
                   touches that table, not just ones needing the blob

PRACTICE: monitor table bloat and autovacuum lag directly, not
          just query latency
impact if skipped: autovacuum falls behind silently until a table
                   scan turns from milliseconds into seconds with no
                   single query change to blame
```

### 3.2 Code level

```
PRACTICE: measure with a profiler before rewriting anything
impact if skipped: optimizing the function you assume is slow while
                   the actual cost sits somewhere else entirely,
                   exactly what a correlation-only diagnosis risks

PRACTICE: match the parsing strategy to payload size, not one
          default for every case
impact if skipped: a validation function that is free at 500 bytes
                   silently becomes the dominant cost at 500KB, no
                   error, no crash, just a CPU chart that climbs

PRACTICE: avoid a full parse-modify-reserialize round trip when only
          one field changes
impact if skipped: two or three full passes over a document to touch
                   a single value, cost scales with document size for
                   no structural reason

PRACTICE: prefer []byte over string at hot-path boundaries when the
          content will be reparsed downstream anyway
impact if skipped: an extra O(n) copy on every conversion; small
                   relative to a full parse, not zero, worth removing
                   once the bigger cost is gone

PRACTICE: bound every unbounded queue or goroutine spawn with an
          explicit limit
impact if skipped: backpressure becomes memory growth instead of a
                   controlled 503, the failure mode moves from
                   "some requests rejected" to "the process OOMs"
```

---

## 4. Highest-Leverage Points, Ranked

```
1. targeted JSON field extraction over full unmarshal
   effort: one file rewritten, one dependency added
   measured impact: CPU 394% -> 79%, throughput +33%, p95 -49%
   this is the single highest-leverage change in the system

2. idempotent producer + acks=all
   effort: three config lines
   impact: eliminates an entire class of duplicate-or-lost-data bugs
   under retry, for the cost of slightly higher produce latency

3. cooperative-sticky rebalancing
   effort: one config value
   impact: removes stop-the-world pauses on every deploy or scale event

4. data-driven event-types.yaml
   effort: a registry pattern, written once
   impact: every future event type costs a config addition, not a
   new handler, new tests, new deploy path

5. consumer lag growth-rate alerting
   effort: one query against metrics already being collected
   impact: earliest possible warning of a downstream slowdown,
   before latency or error rate moves anywhere else

6. dead-letter topic instead of ad hoc error handling
   effort: one additional topic, one routing branch
   impact: a message that would otherwise vanish after retries
   exhaust becomes a reviewable, replayable artifact instead
```

---

## 5. Critical Thinking — Right Instinct vs Wrong Instinct

Specific to decisions actually made in this system, not generic advice.

```
WRONG: "more replicas fixes throughput problems"
why it is wrong here: the ingestion-api was CPU-bound at 394% across
4 cores already allocated to a single instance. Adding more instances
would have masked the code-level inefficiency at proportionally
higher cost, not fixed it. Horizontal scaling multiplies whatever
per-unit cost already exists; it does not change that cost.
RIGHT instinct: profile first, confirm whether the bottleneck is
CPU-bound, I/O-bound, or contention-bound, before deciding whether
more instances even helps.

WRONG: "async/non-blocking is always faster"
why it is wrong here: async concurrency helps when threads are idle
waiting on I/O. A CPU-bound full JSON parse keeps the core busy
regardless of how many goroutines are queued behind it. More
concurrency around CPU-bound work does not create more CPU.
RIGHT instinct: ask which resource is actually scarce before
reaching for a concurrency-model fix.

WRONG: "compression should always be on"
why it is wrong in general: compressing already-dense binary data
costs CPU for negligible size reduction.
why it happens to be right here: the payload is text/JSON, which
typically compresses 70-90%. The right answer is conditional on
payload shape, not a fixed rule either way.
RIGHT instinct: know what you are compressing before deciding
whether compression helps.

WRONG: "exactly-once everywhere is always worth the cost"
why it is wrong here: chasing exactly-once through the JDBC sink to
Postgres is unnecessary complexity when at-least-once plus an
idempotent upsert produces the same practical outcome.
RIGHT instinct: identify the weakest guarantee that still satisfies
the actual requirement, then stop there. Stronger-than-needed
guarantees are a cost paid with no corresponding benefit.

WRONG: "a load test measures the system if it reports success"
why it is wrong here (this happened in this project): a
VU-capped executor was mistaken for a rate-proving one; 50 fixed
virtual users produced a throughput number that reflected the test's
own concurrency ceiling, not the server's real capacity.
RIGHT instinct: understand what your load-testing tool's executor
model actually holds constant before trusting what it reports.

WRONG: "a report with mostly PASS rows means the run passed"
why it is wrong here (also happened in this project): two failing
metrics sat next to eight passing ones with no overall verdict
stated, and the visual impression read as success.
RIGHT instinct: compute and state one explicit pass/fail line against
the actual target, every time, regardless of how many sub-metrics
look fine individually.

WRONG: "the more sophisticated fix must be the right one"
why it is wrong here: gRPC/Protobuf and object-storage offloading
were both real candidate fixes for the CPU bottleneck, but neither
was the actual cause — a profile was needed to find that first.
RIGHT instinct: let the profile or benchmark name the bottleneck
before selecting which fix addresses it. Sophistication is not
evidence of relevance.

WRONG: "the first assumption we wrote down stays true as the system
grows"
why it is wrong here: "payload is 200-500 bytes" was baked into a
correctness claim about validation cost, and stayed unquestioned
until a real load test at 500KB contradicted it directly.
RIGHT instinct: treat early sizing assumptions as falsifiable, and
re-verify them explicitly whenever a parameter that assumption
depended on changes.
```

---

## 6. Numbers Worth Knowing

```
goroutine stack, initial:        ~2 KB
OS thread stack, typical:        ~2-8 MB
L1 cache reference:               ~1 ns          (order-of-magnitude reference,
L2 cache reference:               ~4 ns           commonly cited from Jeff Dean's
main memory reference:            ~100 ns          "numbers every engineer should
SSD random read:                  ~100 us           know" compilation; treat these
same-datacenter network round trip: ~0.5 ms          as intuition anchors, not
cross-region network round trip:  ~50-150 ms         guarantees for your hardware)

Kafka single partition, rough guideline:
  low-to-mid thousands of small messages/sec, or roughly
  10 MB/s, before a second partition helps more than tuning
  the first one further -- always confirm against your own
  message size and hardware, this is a starting intuition only

this system's actual measured numbers, not guidelines:
  394% CPU (4 cores saturated) at 58 req/s -- before the fix
  79% CPU at 77 req/s -- after targeted-parsing fix
  21x benchmark speedup on small payloads, full-unmarshal vs
  targeted extraction
  15 allocations -> 1 allocation, same benchmark
```

---

## 7. Design Patterns Actually Relevant to This System

```
Registry
  used for: event-type config lookup, topology step-type lookup
  shape: a map plus a register function plus a get-or-fail-loudly
  lookup, identical shape in Go and Kotlin

Strategy
  used for: routing an event to a topic based on its declared type,
  the routing logic is selected by data, not by an if/else chain

Idempotent Receiver
  used for: the Redis SETNX dedup check and the Kafka idempotent
  producer, both exist so a retried request cannot double-apply

Chain of Responsibility
  used for: envelope validation -> payload validation -> custom
  processor -> dedup check -> produce, each stage can halt the chain

Circuit Breaker
  relevant but not literally coded in this system's producer path;
  worth adding if a downstream dependency (not Kafka itself, which
  already has its own failure handling) becomes a repeat failure point

Outbox
  not used in this system; relevant if a future requirement needs
  events derived directly from a database write rather than an
  explicit produce call from application code

Bulkhead
  relevant to the per-request concurrency cap (WithRateLimit) --
  isolating how much of the process's resources one code path can
  consume before it starts failing closed instead of degrading
  the whole process
```

---

## 8. Debugging Playbook

### 8.1 Code level — Go

```
CPU profile, 30 second window:
  go tool pprof -seconds=30 http://host:6060/debug/pprof/profile

inside the pprof shell:
  top20 -cum
  list FunctionName
  web
  svg

goroutine dump, full stacks:
  curl http://host:6060/debug/pprof/goroutine?debug=2

heap profile:
  go tool pprof http://host:6060/debug/pprof/heap

scheduler-level trace:
  curl http://host:6060/debug/pprof/trace?seconds=5 > trace.out
  go tool trace trace.out
```

Generic request call-stack tree, the shape to expect when reading a profile or a manual trace of one request through this system:

```
ServeHTTP
├── json.Decoder.Decode                  (envelope parse)
├── RawEvent.Validate                    (envelope-level checks)
├── eventtypes.Get                       (registry lookup)
├── eventtypes.ValidatePayload
│   └── jsonparser.Get                   (per declared field)
├── eventtypes.GetCustomProcessor
│   └── PurchaseEnrichment
│       └── jsonparser.Set               (only if this event type has one)
├── Deduper.SeenBefore
│   └── redis.SetNX
└── Producer.Produce
    ├── encodeAvro
    └── kafka client send/ack wait
```

Reading a profile means matching which of these branches the flame graph shows widest — that width is where the fix belongs, not wherever intuition points first.

### 8.2 Database level — Postgres

```sql
SELECT pid, now() - query_start AS duration, state, query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY duration DESC;

SELECT locktype, relation::regclass, mode, granted
FROM pg_locks
WHERE NOT granted;

SELECT query, calls, total_exec_time, mean_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;

SELECT relname, n_dead_tup, n_live_tup,
       round(n_dead_tup::numeric / GREATEST(n_live_tup, 1), 4) AS dead_ratio
FROM pg_stat_user_tables
ORDER BY dead_ratio DESC;

SELECT indexrelname, idx_scan, idx_tup_read
FROM pg_stat_user_indexes
ORDER BY idx_scan ASC;

SELECT pg_size_pretty(pg_total_relation_size('raw_events'));

SELECT count(*), max(now() - query_start)
FROM pg_stat_activity
WHERE datname = current_database();

SELECT client_addr, state, sent_lsn, replay_lsn,
       pg_wal_lsn_diff(sent_lsn, replay_lsn) AS lag_bytes
FROM pg_stat_replication;
```

### 8.3 Kafka CLI, full working reference

```
Topics
  kafka-topics.sh --bootstrap-server localhost:9092 --list
  kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic events.raw
  kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events.raw --partitions 12 --replication-factor 3
  kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic events.raw --partitions 24
  kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic events.raw

Producing and consuming manually
  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.raw
  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events.raw --from-beginning
  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events.raw --partition 0 --offset 100

Consumer groups
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group stream-processor
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group stream-processor --reset-offsets --to-earliest --topic events.raw --execute
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group stream-processor --reset-offsets --shift-by -1000 --topic events.raw --execute

Dynamic config
  kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type topics --entity-name events.raw
  kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name events.raw --add-config retention.ms=604800000
  kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type brokers --entity-name 1

Partition reassignment
  kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --generate --topics-to-move-json-file topics.json --broker-list 1,2,3
  kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --execute --reassignment-json-file reassignment.json
  kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --verify --reassignment-json-file reassignment.json

Disk and log inspection
  kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe --topic-list events.raw
  kafka-dump-log.sh --files /var/kafka/data/events.raw-0/00000000000000000000.log --print-data-log

Cluster and broker introspection
  kafka-broker-api-versions.sh --bootstrap-server localhost:9092
  kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status

Lightweight alternative for quick checks
  kcat -b localhost:9092 -L
  kcat -b localhost:9092 -t events.raw -C -o beginning
  kcat -b localhost:9092 -t events.raw -P
```

---

## 9. Configuration Reference — Generic, Any-Language

Every block below is followed by what each key does and what changing it actually causes — not just what it is.

### 9.1 Broker

```yaml
broker:
  node.id: 1
  process.roles: [broker, controller]
  replication.factor: 3
  min.insync.replicas: 2
  unclean.leader.election.enable: false
  rack: az-1
  log.retention.hours: 168
  log.segment.bytes: 1073741824
  message.max.bytes: 1048576
  num.io.threads: 8
  num.network.threads: 3
```

```
node.id: unique broker identity
  change effect: colliding IDs across brokers prevents cluster
  formation entirely, not a degraded state, a refusal to start

replication.factor: copies of each partition
  change effect: raising it increases durability and disk usage
  linearly; lowering it below 3 removes the tolerance for a single
  broker failure

min.insync.replicas: replicas that must ack before a write with
acks=all succeeds
  change effect: raising it toward replication.factor trades
  availability for durability, equal to RF means any single broker
  loss halts writes; lowering it toward 1 trades durability for
  availability, risking the diverging-replica scenario in section 1

unclean.leader.election.enable: allow an out-of-sync replica to
become leader when no in-sync replica is available
  change effect: true trades availability for silent data loss;
  false means an affected partition goes visibly offline instead,
  which is the correct trade for this system's durability goal

rack: failure-domain label per broker
  change effect: unset means Kafka may place every replica of a
  partition in the same availability zone, silently defeating the
  purpose of replication.factor > 1

log.retention.hours: how long a message stays on disk
  change effect: shorter risks a slow consumer falling off the front
  of the log before it catches up; longer increases disk usage
  linearly with no throughput effect

message.max.bytes: largest message the broker will accept
  change effect: too low silently rejects legitimate large payloads
  at the client with a size error; must be raised in lockstep with
  the producer's own max-request-size setting or the two disagree
  and the smaller one wins invisibly
```

### 9.2 Producer

```yaml
producer:
  acks: all
  enable.idempotence: true
  retries: 5
  delivery.timeout.ms: 30000
  linger.ms: 8
  batch.size: 65536
  compression.type: lz4
  max.in.flight.requests.per.connection: 5
  max.request.size: 1048576
```

```
acks: durability level the producer waits for
  change effect: 0 is fire-and-forget, fastest, loses data silently
  on any broker failure; 1 waits for the leader only; all waits for
  every in-sync replica, the floor this system requires

enable.idempotence: broker-side dedup by producer id and sequence
  change effect: false means retrying an ambiguous timeout can create
  a genuine duplicate on the broker; true makes that retry a safe
  no-op instead

linger.ms: time the producer waits to batch more records before
sending
  change effect: 0 sends immediately, one network round trip per
  record; raising it batches more concurrent sends into fewer round
  trips, at the cost of that many milliseconds of added latency per
  record

batch.size: max bytes per batch before it is sent regardless of
linger.ms
  change effect: too small relative to message size means every
  message becomes its own batch, batching provides zero benefit;
  must be sized against actual payload size, not left at a default
  tuned for a different payload shape

compression.type: codec applied per batch
  change effect: helps materially on compressible payloads like
  text/JSON, wastes CPU for near-zero gain on already-compressed
  binary data; the right value is conditional on payload shape

max.in.flight.requests.per.connection: concurrent unacknowledged
requests allowed per connection
  change effect: values above 5 with idempotence enabled can reorder
  batches on retry; 5 is the documented safe ceiling for that
  combination

max.request.size: largest single request the client will send
  change effect: must exceed the largest payload actually sent, and
  must not exceed the broker's message.max.bytes, or requests fail
  client-side or broker-side respectively with no automatic
  reconciliation between the two settings
```

### 9.3 Consumer

```yaml
consumer:
  group.id: stream-processor
  enable.auto.commit: false
  partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
  max.poll.records: 500
  max.poll.interval.ms: 300000
  session.timeout.ms: 45000
  fetch.min.bytes: 1
  auto.offset.reset: earliest
```

```
group.id: identity of the consumer group for offset tracking and
partition assignment
  change effect: changing this for an existing deployment is
  equivalent to starting a brand-new consumer with no history,
  offsets committed under the old group.id are invisible to it

enable.auto.commit: whether offsets commit on a timer instead of
after explicit processing
  change effect: true risks committing an offset for a record that
  crashed mid-processing, silently losing it; false requires the
  application to commit explicitly at the correct point, section 1

partition.assignment.strategy: how partitions are divided among
group members during a rebalance
  change effect: the default eager strategy pauses every consumer in
  the group on every rebalance; cooperative-sticky only pauses the
  consumers whose assignment actually changes

max.poll.interval.ms: maximum time allowed between poll() calls
before the consumer is considered dead
  change effect: too low relative to actual per-batch processing time
  causes healthy consumers to be evicted mid-work, triggering
  unnecessary rebalances under normal load, not just real failures

auto.offset.reset: where a consumer with no committed offset starts
  change effect: earliest reprocesses the entire retained log on
  first start, guaranteeing no gap but potentially reprocessing a
  large backlog; latest starts from now, guaranteeing no reprocessing
  but silently skipping anything produced before the consumer existed
```

### 9.4 Serializer / Deserializer

```yaml
serde:
  key.serializer: org.apache.kafka.common.serialization.StringSerializer
  value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
  schema.registry.url: http://schema-registry:8081
  auto.register.schemas: false
  specific.avro.reader: true
```

```
value.serializer: the codec applied to the message body
  change effect: switching format after messages already exist on a
  topic breaks every consumer expecting the old format on the next
  read, this is a breaking migration, never a drop-in swap

schema.registry.url: where schema IDs are registered and resolved
  change effect: unreachable at produce time blocks every produce
  call that needs to register or resolve a schema id, this becomes a
  hard dependency, not an optional one

auto.register.schemas: whether a producer can silently register a
new schema version on send
  change effect: true lets an unreviewed schema change reach
  production the moment a producer restarts with new code; false
  forces an explicit registration step, catching an incompatible
  change before it reaches the topic

specific.avro.reader: deserialize into generated typed classes
instead of a generic record
  change effect: false returns a loosely-typed GenericRecord, pushing
  type-safety checks to runtime instead of compile time
```

### 9.5 Event / topic management

```yaml
topic_management:
  cleanup.policy: delete
  retention.ms: 604800000
  min.compaction.lag.ms: 0
  segment.ms: 604800000
  max.message.bytes: 1048576
```

```
cleanup.policy: delete vs compact
  change effect: delete removes data older than retention.ms
  entirely; compact instead keeps only the latest value per key
  forever, appropriate for a changelog topic, wrong for an
  append-only event stream where every event matters independently

retention.ms: how long delete-policy data survives
  change effect: shorter than the time it takes a slow or down
  consumer to catch up means permanent data loss for that consumer,
  not just delay

segment.ms: how often the active log segment rolls over
  change effect: longer intervals mean fewer, larger files, less
  filesystem overhead but slower application of retention since a
  whole segment must age out before it is deleted
```

### 9.6 Database management

```yaml
database:
  pool.max_size: 20
  pool.min_size: 2
  statement_timeout.ms: 5000
  idle_in_transaction_timeout.ms: 10000
  connect_timeout.ms: 3000
```

```
pool.max_size: ceiling on concurrent database connections from one
service instance
  change effect: too high across many replicas exhausts the
  database's own max_connections, turning the database into the
  outage; too low serializes requests behind a connection wait

statement_timeout.ms: hard ceiling on any single query's runtime
  change effect: unset allows one pathological query to hold a
  connection indefinitely, starving the pool for every other request
  regardless of how well-sized pool.max_size is

idle_in_transaction_timeout.ms: kills a connection left open inside
an uncommitted transaction
  change effect: unset lets a bug that opens a transaction and never
  closes it hold locks indefinitely, blocking other writers with no
  visible query to blame
```

### 9.7 Service dependency ordering

```yaml
startup_order:
  postgres:
    healthcheck: pg_isready
  redis:
    healthcheck: redis_ping
  kafka:
    depends_on: []
    healthcheck: broker_api_versions
  schema_registry:
    depends_on: [kafka]
  kafka_connect:
    depends_on: [kafka, postgres, schema_registry]
  ingestion_api:
    depends_on: [kafka, redis]
  stream_processor:
    depends_on: [kafka, schema_registry]
```

```
depends_on without a healthcheck condition: guarantees container
START order only, not readiness
  change effect: a dependent service can start and immediately fail
  its first request against a dependency that is running but not yet
  accepting connections, e.g. Kafka's controller still forming quorum

healthcheck-gated depends_on: blocks dependent startup until the
dependency reports actually ready
  change effect: slower cold start across the whole stack, in
  exchange for eliminating an entire class of startup-order race
  conditions that otherwise only show up intermittently
```

### 9.8 Networking and ports

```yaml
networking:
  ingestion_api.port: 8080
  kafka.listener.internal: 9092
  kafka.listener.controller: 9093
  schema_registry.port: 8081
  kafka_connect.port: 8083
  postgres.port: 5432
  redis.port: 6379
  advertised_listeners: PLAINTEXT://kafka:9092
```

```
advertised_listeners: the address Kafka tells clients to use for
subsequent connections, separate from the address it binds to
  change effect: set to an address unreachable from where clients
  actually run (a common misconfiguration is leaving this as
  localhost inside a container) and every client connects for the
  initial handshake, then fails on every subsequent request when it
  tries to follow the advertised address

exposing broker or database ports to a public network instead of an
internal one
  change effect: not a performance question, a direct security
  exposure; internal-only service ports should never carry a public
  binding regardless of how convenient it is for local debugging
```
## 10. Billion-User Scale — What Actually Changes

The foundation above (sections 1-9) is necessary at any scale and is not being discarded — a CPU-bound JSON parser is still a CPU-bound JSON parser whether you run 3 instances or 30,000. What changes at billion-user scale is that per-instance efficiency stops being sufficient by itself; new failure modes appear that no amount of single-cluster tuning solves.

Capacity math, assumptions stated explicitly rather than presented as fact:

```
assumed: 1,000,000,000 registered users
assumed: 20% daily active -> 200,000,000 DAU
assumed: 50 events/user/day (page views, actions, writes)

derived: 200,000,000 x 50 = 10,000,000,000 events/day
derived: 10e9 / 86400 seconds = ~115,700 events/sec sustained average
derived: with a 5x peak factor for daily usage waves across timezones
         = ~580,000 events/sec peak design target
```

That peak number is 100-350x the original 1,667-5,000 req/s target from section 1 — a genuine change in kind, not just a bigger version of the same problem. For calibration against a real, publicly documented deployment rather than an assumption-based estimate: LinkedIn's Kafka infrastructure processes over 7 trillion messages per day (roughly 81 million messages/sec average) across more than 100 separate Kafka clusters, 4,000+ brokers, and 7 million partitions — not one enormous cluster. That architectural choice (many clusters, not one) is itself the answer to the first decision tree below, and it is a choice forced by real operational limits at that scale, not a preference.

---

## 11. Advanced Decision Trees — Billion-Scale Architecture

```
DECISION: One global Kafka cluster vs many regional clusters + replication
│
├── branch: single global cluster / one cluster per region with
│           MirrorMaker2 replication / fully independent regional
│           clusters with no cross-region replication at all
│
├── root cause
│   └── sequence flow:
│         a single cluster's practical ceiling is governed by
│         controller metadata load and cross-region network latency
│         for replication, not just broker count
│         -> min.insync.replicas=2 across continents adds hundreds
│            of milliseconds to every acks=all produce call
│         -> the durability guarantee itself becomes the latency
│            problem, not a separate concern from it
│
└── trade-offs
    └── why one cluster per region, replicated, not one global cluster:
          single global cluster: durability requires cross-continent
          replication on the hot path, adding hundreds of ms to
          every produce call regardless of where the request
          originated
          fully independent, unreplicated regional clusters: fine
          for genuinely region-local data, breaks the moment any
          workload needs a cross-region view -- a global aggregate,
          or a user who travels
          one cluster per region, MirrorMaker2 (KIP-382, the
          established production-proven approach, now running on
          KRaft as of Kafka 4.0+) replicating asynchronously between
          them: local produce/consume stays at local latency,
          cross-region visibility lags by replication delay but
          never sits on any single request's critical path.
          a newer native alternative, Cluster Mirroring (KIP-1279),
          is in active development as of early 2026 and not yet
          generally available -- worth tracking, not yet a default
          choice
```

```
DECISION: Database scaling strategy past a single Postgres primary
│
├── branch: vertical scaling (bigger instance) / read replicas only /
│           Citus-style horizontal sharding / move to a different
│           storage engine entirely
│
├── root cause
│   └── sequence flow:
│         a single Postgres primary has a write-throughput ceiling
│         set by disk I/O and single-machine CPU
│         -> vertical scaling has a hard ceiling; there is no
│            infinitely large single machine to buy
│         -> read replicas solve read scaling, not write scaling,
│            and the ingestion path in this system is overwhelmingly
│            writes
│
└── trade-offs
    └── why Citus-style horizontal sharding over the alternatives:
          vertical scaling: simplest operationally, hits a real
          ceiling, and that ceiling arrives with no graceful
          degradation path once reached
          read replicas alone: does nothing for the write path that
          this ingestion system actually stresses
          horizontal sharding (Citus, an actively maintained,
          100% open-source Postgres extension providing both
          row-based sharding by a distribution column and
          schema-based sharding for multi-tenant workloads with no
          distribution key required): keeps standard Postgres SQL
          and tooling, distributes both storage and query
          parallelism across nodes, at the cost of needing a
          deliberate shard key choice, see the next tree
          moving to a different engine entirely (Cassandra,
          ScyllaDB): justified only if the access pattern is
          fundamentally write-heavy key-value with no need for
          Postgres's relational/transactional features -- a bigger
          migration than the problem usually requires if Citus
          already solves it
```

```
DECISION: Consistency model, per data class rather than one global choice
│
├── branch: strong consistency everywhere / eventual consistency
│           everywhere / per-data-class choice
│
├── root cause
│   └── sequence flow:
│         CAP theorem is not optional at multi-region scale: a
│         network partition between regions WILL happen eventually
│         -> the system must have already decided, in advance,
│            whether it favors consistency or availability during
│            that partition, for each kind of data it holds
│
└── trade-offs
    └── why per-data-class, not one blanket answer:
          strong consistency everywhere: correct for financial
          balances, account state, anything where a stale read is a
          real-world wrong answer -- but paying synchronous
          cross-region consensus cost for a view counter is waste,
          not correctness
          eventual consistency everywhere: correct for aggregate
          counts, analytics, recommendation feeds -- but applied to
          an account balance, it is a bug, not a trade-off
          per-data-class: financial/critical state uses synchronous
          replication or a consensus protocol and accepts the
          latency cost; counts/analytics/feeds use CRDTs or simple
          async replication and accept the staleness window. One
          system, two honestly different guarantees, chosen
          deliberately per data type rather than by accident
```

```
DECISION: Global admission control / rate limiting
│
├── branch: per-instance local rate limiting only / fully centralized
│           rate limiter / local limiting with periodic global sync
│
├── root cause
│   └── sequence flow:
│         a single global rate limit enforced by one centralized
│         service becomes a single point of contention that every
│         request must round-trip to before proceeding
│         -> at hundreds of thousands of req/sec across many regions,
│            that round trip alone can exceed the latency budget
│
└── trade-offs
    └── why local limiting with periodic global sync:
          per-instance-local-only: fast, zero coordination cost, but
          a global limit of X req/sec becomes X times the instance
          count in the worst case if load is unevenly distributed
          fully centralized: accurate, but the coordination round
          trip becomes latency on every request and the limiter
          service itself becomes a new single point of failure
          local limiting with periodic sync (each instance enforces
          limit/instance_count locally, and instances periodically
          exchange actual usage to rebalance that local share):
          approximate rather than exact, trades precision for
          removing coordination from the hot path entirely -- the
          right trade when "approximately correct, always fast" beats
          "exactly correct, sometimes slow"
```

```
DECISION: Shard key selection for user data
│
├── branch: user_id / tenant_id / composite key / random or
│           round-robin assignment
│
├── root cause
│   └── sequence flow:
│         a single Postgres shard cannot hold billion-user write
│         volume
│         -> every event needs a deterministic function mapping it
│            to exactly one shard
│         -> that function must not create a hot shard under
│            realistic, skewed usage
│
└── trade-offs
    └── why user_id, with explicit hot-key mitigation as a second
        deliberate layer, not a single unmitigated choice:
          random/round-robin: near-perfect distribution, but breaks
          per-user data co-location, turning every per-user query
          into a scatter-gather across every shard
          tenant_id alone: fine while tenants are similarly sized,
          fails the moment one tenant is orders of magnitude larger
          than the rest -- that tenant becomes a permanently hot shard
          user_id alone: good default distribution and keeps a
          user's own data together, but a small number of viral or
          celebrity accounts can still overwhelm a single shard --
          this is not a reason to abandon user_id, it is a reason to
          add hot-key detection and splitting on top of it, see
          section 13
```

```
DECISION: Disaster recovery target, set per data class
│
├── branch: one blanket RTO/RPO for everything / per-data-class
│           targets / best-effort with no explicit target
│
├── root cause
│   └── sequence flow:
│         each additional nine of RPO/RTO tightness multiplies cost,
│         it does not simply add to it -- synchronous cross-region
│         replication for near-zero RPO costs latency on every
│         single write, every day, not only during an actual disaster
│
└── trade-offs
    └── why per-data-class targets:
          one blanket tight target for all data: pays the most
          expensive tier's cost for data that never needed it
          no explicit target at all: the target gets decided
          accidentally, during the first real incident, under the
          worst possible conditions to be making that decision
          per-data-class (tight RPO with synchronous replication for
          financial/critical data; minutes-scale RPO with async
          MirrorMaker2 replication for analytics/aggregate data):
          pays for durability precisely where the cost of losing it
          is actually high
```

```
DECISION: Schema evolution governance across many teams
│
├── branch: unrestricted self-service registration / CI-enforced
│           compatibility checks / centralized manual review board
│
├── root cause
│   └── sequence flow:
│         hundreds of teams eventually produce or consume the same
│         topics
│         -> one team's field rename, judged harmless by that team,
│            silently breaks every other team's consumer
│         -> that breakage is discovered in production, not in code
│            review, because no automated check ran between the two
│
└── trade-offs
    └── why CI-enforced compatibility checking, not a manual board:
          unrestricted: fast for the changing team, an incident for
          every downstream team, discovered late
          manual review board: correct in principle, becomes an
          organizational bottleneck at hundreds-of-teams scale, and
          teams route around it under deadline pressure once it does
          auto.register.schemas=false plus a CI step that checks the
          proposed schema against the registry's configured
          compatibility mode (BACKWARD/FULL) before merge: catches
          the same class of problem at commit time, and scales with
          team count because no human needs to be in the loop for
          the common case
```

```
DECISION: Trace sampling strategy at hundreds-of-thousands-of-events/sec
│
├── branch: 100% sampling / fixed-percentage sampling / tail-based
│           sampling / adaptive sampling
│
├── root cause
│   └── sequence flow:
│         100% tracing at this volume makes trace-backend storage
│         and ingest cost scale linearly with production traffic --
│         it becomes one of the largest infrastructure line items,
│         not a rounding error
│
└── trade-offs
    └── why tail-based sampling plus a small flat baseline:
          fixed-percentage (e.g. 1%): cheap, simple, but samples
          errors and slow requests at the same rate as routine ones
          -- the traces most needed during an incident are exactly
          as likely to have been discarded as any other
          tail-based: the sampling decision is made AFTER a trace
          completes, keeping error and high-latency traces at a much
          higher rate than routine ones, at the cost of buffering
          every span until that decision can be made
          a small flat baseline kept alongside tail-based rules
          exists specifically to catch problems that manifest as
          neither an error nor an obvious latency spike
```

```
DECISION: Kafka retention storage tiering
│
├── branch: all data on broker-local disk / short local retention
│           with no tiering / tiered storage to object storage
│
├── root cause
│   └── sequence flow:
│         compliance, replay, and reprocessing needs often want
│         weeks-to-months of retained history
│         -> broker-local disk sized for that retention at
│            billion-event volume becomes the single largest
│            infrastructure cost line by a wide margin
│
└── trade-offs
    └── why tiered storage (KIP-405, generally available in current
        Kafka via remote.log.storage.system.enable=true):
          all-local: fastest access to recent data, disk cost scales
          linearly with retention window and volume -- the most
          expensive option at this scale
          short retention, no tiering: cheapest, but anything needing
          to replay history beyond that window simply cannot -- a
          real constraint on bug investigation and reprocessing, not
          a theoretical one
          tiered storage: recent segments stay on local disk for
          fast access, older segments move automatically to
          S3/GCS/HDFS, letting retention extend far longer at
          object-storage prices instead of broker-disk prices
```

---

## 12. Distributed Systems Algorithms — Pseudocode

Generic Python-style throughout, matching section 9's convention.

### 12.1 Consistent hashing ring, for shard assignment

```python
def build_ring(shard_count, virtual_nodes_per_shard):
    ring = SortedMap()
    for shard_id in range(shard_count):
        for v in range(virtual_nodes_per_shard):
            token = hash_function(f"{shard_id}:{v}")
            ring[token] = shard_id
    return ring

def get_shard(ring, key):
    token = hash_function(key)
    position = ring.ceiling_key(token)
    if position is None:
        position = ring.first_key()
    return ring[position]

def add_shard(ring, new_shard_id, virtual_nodes_per_shard):
    for v in range(virtual_nodes_per_shard):
        token = hash_function(f"{new_shard_id}:{v}")
        ring[token] = new_shard_id
    return ring
```

Virtual nodes per physical shard exist specifically so that adding or removing one shard redistributes an even fraction of keys across every remaining shard, rather than dumping the whole burden onto whichever single shard happens to be adjacent on the ring.

### 12.2 Distributed sliding-window rate limiter

```python
def sliding_window_allow(redis_client, key, limit, window_seconds):
    now_ms = current_time_ms()
    window_start_ms = now_ms - (window_seconds * 1000)
    pipeline = redis_client.pipeline()
    pipeline.zremrangebyscore(key, 0, window_start_ms)
    pipeline.zadd(key, {generate_request_id(): now_ms})
    pipeline.zcard(key)
    pipeline.expire(key, window_seconds)
    results = pipeline.execute()
    request_count = results[2]
    return request_count <= limit
```

Atomic Lua-script version, avoiding the race window between separate commands entirely:

```
local current = redis.call('INCR', KEYS[1])
if tonumber(current) == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
if tonumber(current) > tonumber(ARGV[2]) then
    return 0
end
return 1
```

### 12.3 CRDT grow-only counter, for eventually-consistent cross-region aggregates

```python
class GCounter:
    def __init__(self, region_id):
        self.region_id = region_id
        self.counts_by_region = {}

    def increment(self, amount=1):
        current = self.counts_by_region.get(self.region_id, 0)
        self.counts_by_region[self.region_id] = current + amount

    def value(self):
        return sum(self.counts_by_region.values())

    def merge(self, other):
        merged = GCounter(self.region_id)
        all_regions = set(self.counts_by_region) | set(other.counts_by_region)
        for region in all_regions:
            merged.counts_by_region[region] = max(
                self.counts_by_region.get(region, 0),
                other.counts_by_region.get(region, 0),
            )
        return merged
```

Merge takes the max per region rather than summing the two totals, which is what makes this convergent regardless of how many times or in what order two replicas merge with each other.

### 12.4 Region leader election / failover

```python
def region_health_monitor(candidate_regions, lease_store):
    while running:
        current_leader = lease_store.get_current_leader()
        if current_leader is None or lease_store.is_expired(current_leader):
            candidate = select_healthiest(candidate_regions)
            acquired = lease_store.try_acquire(candidate.region_id, ttl_seconds=15)
            if acquired:
                promote_to_active(candidate)
                demote_all_except(candidate_regions, candidate.region_id)
        elif lease_store.owner_is_self(current_leader):
            lease_store.renew(current_leader, ttl_seconds=15)
        sleep_ms(5000)
```

The short TTL relative to the poll interval is deliberate: a leader that has actually failed is detected and replaced within roughly one lease period, while a healthy leader renews well before expiry under normal operation.

### 12.5 Priority-based load shedding

```python
def admit_request(request, current_load, capacity):
    priority = classify_priority(request)
    utilization = current_load / capacity
    if utilization < 0.7:
        return ADMIT
    if utilization < 0.9:
        return ADMIT if priority in (CRITICAL, HIGH) else REJECT
    return ADMIT if priority == CRITICAL else REJECT
```

Degradation happens in explicit stages rather than a single all-or-nothing cutover, so the system sheds low-priority load well before it is forced to shed everything.

---

## 13. Sharding & Hot-Key Mitigation Deep Dive

```python
def route_with_hot_key_mitigation(key, ring, hot_key_registry):
    if hot_key_registry.is_hot(key):
        sub_shards = hot_key_registry.get_sub_shards(key)
        chosen = sub_shards[hash_function(key + random_salt()) % len(sub_shards)]
        return chosen
    return get_shard(ring, key)

def detect_hot_keys(load_metrics, threshold_multiplier, hot_key_registry):
    average_load = load_metrics.mean_load_per_key()
    for key, load in load_metrics.current_loads():
        if load > average_load * threshold_multiplier:
            assigned_sub_shards = allocate_sub_shards(key, count=8)
            hot_key_registry.mark_hot(key, assigned_sub_shards)
            alert("hot key detected and split", key, load)
```

A hot key gets fanned out across several sub-shards instead of moved to a bigger single shard — a bigger single shard just becomes tomorrow's hot key at a higher absolute load. Detection has to be continuous, not a one-time provisioning decision, because which keys are hot changes with real-world virality and cannot be predicted in advance.

---

## 14. Advanced Critical Thinking — Biases at Billion-User Scale

```
WRONG: "billion-user architecture is just the same design with more
of everything"
why it is wrong: adding more Go instances around a single Postgres
primary and a single Kafka cluster does not solve write-throughput
ceilings, cross-region latency, or the CAP-theorem trade that a
network partition eventually forces. These are qualitatively
different problems, not a bigger quantity of the same problem.
RIGHT instinct: identify which constraints are structural (a single
machine's disk I/O ceiling, the speed of light between continents)
versus which are just under-provisioned, before assuming more
capacity of the same shape fixes it.

WRONG: "always design for billion-user scale from day one"
why it is wrong: this is the mirror-image bias of the one above, and
it happens just as often. Multi-region active-active replication,
sharded databases, and cross-cluster mirroring are real, ongoing
operational cost and complexity paid on every deploy, every incident,
every onboarding of a new engineer -- whether or not the traffic
exists yet to justify it. The foundation in sections 1-9 was
correctly right-sized for its stated 1,667 req/s target; building
this section's architecture against that target would have been the
same mistake in the other direction.
RIGHT instinct: build for the load that is actually coming in a
defensible timeframe, with clear seams where the next tier of scale
can be added -- not the load that might theoretically exist someday.

WRONG: "eventual consistency is simpler, so default to it everywhere"
why it is wrong: eventual consistency does not remove complexity, it
relocates it -- from the write path (where synchronous consistency
would have paid the cost once) to every reader, who must now handle
staleness, conflict resolution, and "which value is correct right
now" as an application-level concern, forever, on every read.
RIGHT instinct: eventual consistency is a trade, not a simplification
-- evaluate where the complexity actually lands, not just whether the
write path looks simpler.

WRONG: "a shard key that distributes evenly today will always
distribute evenly"
why it is wrong: real-world usage is not static. A perfectly uniform
distribution under today's usage pattern can develop a hot shard the
moment one account goes viral, with no code change and no warning
beyond a load metric that was not being watched for this specific
failure mode.
RIGHT instinct: hot-key detection is a continuous, permanent process,
not a one-time capacity-planning exercise performed at design time.

WRONG: "more regions always means more availability"
why it is wrong: every additional region is also an additional
failure domain, an additional replication target, and an additional
place a network partition can occur between. Past a certain point,
coordination overhead between regions can make the system LESS
available than fewer, well-run regions would have been.
RIGHT instinct: each additional region should be justified by an
actual latency or regulatory requirement in that geography, not
added by default because "more regions sounds more resilient."

WRONG: "the biggest, most sophisticated companies' architecture is
the correct target to copy"
why it is wrong: LinkedIn runs 100+ separate Kafka clusters because
their actual measured scale (7 trillion messages/day) forces that
architecture. Adopting a 100-cluster topology at a fraction of that
volume imports LinkedIn's operational complexity without importing
the traffic that justifies it.
RIGHT instinct: use published numbers from real deployments to
calibrate what problem looks like at what scale -- not as a target
architecture to copy regardless of whether your actual numbers
justify it.
```

---

## 15. Advanced Query Patterns — Sharded & Distributed Data

### 15.1 Scatter-gather across shards

```python
def scatter_gather_query(shard_connections, query_template, params):
    pending = []
    for shard in shard_connections:
        pending.append(async_execute(shard, query_template, params))
    results = wait_all(pending, timeout_ms=2000)
    return merge_results(results)

def merge_results(shard_results):
    merged_rows = []
    for result in shard_results:
        if result.succeeded:
            merged_rows.extend(result.rows)
        else:
            record_partial_failure(result.shard_id, result.error)
    return merged_rows
```

A scatter-gather query's latency is bounded by its slowest responding shard, not its average one — this is the direct cost of denormalizing a query across shards, and it is why per-user queries staying inside one shard (section 11's shard-key tree) matters more as shard count grows.

### 15.2 Time-range table partitioning, for billion-row retention management

```sql
CREATE TABLE raw_events (
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
) PARTITION BY RANGE (occurred_at);

CREATE TABLE raw_events_2026_08 PARTITION OF raw_events
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE raw_events_2026_09 PARTITION OF raw_events
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

DROP TABLE raw_events_2026_08;
```

`DROP TABLE` on an aged-out partition is a metadata-only operation regardless of how many billions of rows that partition holds. The alternative — a `DELETE ... WHERE occurred_at < X` against an unpartitioned billion-row table — scans and removes rows individually, at a cost that scales with row count rather than being close to constant.

### 15.3 Distributed advisory lock, for cross-shard coordination

```sql
SELECT pg_try_advisory_lock(hashtext('shard-rebalance'));

SELECT pg_advisory_unlock(hashtext('shard-rebalance'));
```

The try-variant returns immediately with true or false instead of blocking, which matters for a coordination task like a rebalance: the caller can decide to back off and retry later rather than queueing up a wait on something that might legitimately take minutes.

### 15.4 Shard-aware connection routing

```python
def get_connection_for_key(key, shard_topology):
    shard_id = get_shard(shard_topology.ring, key)
    return shard_topology.connection_pools[shard_id].acquire()

def execute_routed(query, key, params, shard_topology):
    connection = get_connection_for_key(key, shard_topology)
    try:
        return connection.execute(query, params)
    finally:
        connection.release()
```

---

## 16. Advanced Kafka & Infrastructure CLI — Multi-Cluster Operations

### 16.1 Cross-cluster replication, MirrorMaker2

```
clusters = us-east, eu-west
us-east.bootstrap.servers = kafka-us-east:9092
eu-west.bootstrap.servers = kafka-eu-west:9092
us-east->eu-west.enabled = true
eu-west->us-east.enabled = true
replication.factor = 3
sync.topic.acls.enabled = true
```

```
connect-mirror-maker.sh mm2.properties
```

### 16.2 Tiered storage enablement (KIP-405, generally available)

```
remote.log.storage.system.enable = true
remote.log.storage.manager.class.name = org.apache.kafka.server.log.remote.storage.RemoteStorageManager
```

### 16.3 Per-tenant client quotas

```
kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type clients --entity-name tenant-42 --add-config producer_byte_rate=1048576,consumer_byte_rate=2097152

kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type clients --entity-name tenant-42
```

### 16.4 ACLs for multi-tenant isolation

```
kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:tenant-42 --operation Write --topic events.raw

kafka-acls.sh --bootstrap-server localhost:9092 --list --topic events.raw
```

### 16.5 Cluster-wide metadata and quorum introspection

```
kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status

kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication
```

### 16.6 Cross-cluster topic configuration drift check

```
kafka-configs.sh --bootstrap-server kafka-us-east:9092 --describe --entity-type topics --entity-name events.raw

kafka-configs.sh --bootstrap-server kafka-eu-west:9092 --describe --entity-type topics --entity-name events.raw
```

Run both and diff the output by hand or in a script — MirrorMaker2's `sync.topic.acls.enabled` covers ACLs, not arbitrary topic configuration; retention, partition count, and other settings can silently drift between regions unless something explicitly checks for it.

---

## 17. Numbers at Billion Scale

```
this document's illustrative capacity estimate (stated assumptions,
not measured fact):
  ~115,700 events/sec sustained average
  ~580,000 events/sec peak design target

LinkedIn's actual, publicly documented Kafka deployment:
  7+ trillion messages/day
  ~81,000,000 messages/sec average, across the whole deployment
  100+ separate Kafka clusters, not one
  4,000+ brokers
  7,000,000 partitions
  ~100,000 topics
  message size capped at 1 MB
  each message consumed by ~4 applications on average

what that comparison shows: even a hypothetical billion-user product
at this document's illustrative estimate sits roughly two orders of
magnitude below LinkedIn's real, measured scale -- and LinkedIn still
runs 100+ separate clusters rather than one. The "many clusters, not
one enormous cluster" architecture in section 11 is not a
theoretical recommendation, it is what the one publicly documented
deployment actually operating near this scale does.
```