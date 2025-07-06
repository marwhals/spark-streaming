# Spark Streaming First Principles

---

### Spark Architecture

TODO add associated diagram 

---

## Motivations

- Once we compute something valuable, we want updates
- We need continuous big data processing
  - This is the rationale behind Spark streaming.

---

## Stream Processing
- Include new data to compute a result
- No definitive end of incoming data
- Batch processing = operate on a fixed, big dataset. Computer the result once.
- In practice, stream and batch interoperate
  - Incoming data joined with a fixed dataset
  - Output of a streaming job periodically queried by a batch job
  - Consistency between batch/streaming jobs.

---
## Stream Processing - Real Life
- Real life examples / use-cases
  - Sensor readings
  - Interactions with an application / website
  - Credit card transactions
  - Real-time dashboards.
  - Alerts and notifications
  - Incremental big data
  - Incremental transactional data e.g. analytics and accounts.
  - Interactive machine learning

---

## Pros / Cons

- Pros
  - Much lower latency than batch processing
  - Greater performance / efficiency (especially with incremental data)
- Difficulties
  - Maintaining state and order for incoming data (*event time processing*)
  - Exactly-once processing in the context of machine failures (*fault tolerance*)
  - Responding to events at low latency
  - Transactional data at runtime
  - Updating business logic at runtime

---

## Spark Streaming Principles

- Declarative API
  - Write "what" need to be computed, let the library decide "how"
  - alternative: RaaT (record-at-a-time)
    - set of APIs to process each incoming element as it arrives
    - low-level: maintaining state and resource usage is your responsibility
    - hard to develop
- Event time vs Processing time API
  - event time = when the event was produced
  - processing time = when the event arrives
  - event time is critical: allows detection of late data points
- Continuous vs micro-batch execution
  - Continuous = include each data point as it arrives (lower latency)
  - micro-batch = wait for a few data points, process them all in the new result (higher throughput)
- Low-level (DStreams) vs High-level API (Structured Streaming)

Spark Streaming operates on micro-batches (continuous execution is experimental)
TODO - check current state of "continuous execution"