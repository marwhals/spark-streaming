# Streaming DataFrames

---

- High-level API
  - Ease of development
  - interoperable with other Spark APIs
  - auto-optimisations

---

## Structured Streaming Principles
- Lazy evaluation
- Transformations and Actions
  - transformations describe how new DFs are obtained
  - actions start executing/running spark code
- Input Sources
  - Kafka, Flume
  - A distributed file system
  - Sockets
- Output sinks
  - A distributed files system
  - Databases
  - Kafka
  - Testing sinks. e.g: console, memory

---

## Streaming I/O
- Output modes
  - append = only add new records
  - update = modify records in place ---- (if query has no aggregations, equivalent with append)
  - complete = rewrite everything
- Not all queries and sinks support all output modes
  - example: aggregations and append mode
- Triggers = when new data is writing
  - default: write as soon as the current micro-batch has been processed
  - once: write a single micro-batch and stop
  - processing-time: look for new data at fixed intervals
  - continuous (currently experimental)
