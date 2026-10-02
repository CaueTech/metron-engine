# Metron Engine

Metron Engine is a distributed event processing engine designed for real-time machine telemetry, validation, and anomaly detection. 

The project simulates and processes high-throughput telemetry streams using Apache Kafka and Protocol Buffers. It includes a dedicated chaos generator capable of injecting realistic system anomalies—such as malformed payloads, invalid timestamps, and duplicate identifiers—to test system resilience, routing, and Dead Letter Queue (DLQ) handling under adverse conditions.

---

## Architecture Overview

- **Generator (Chaos Producer):** Simulates sensor/host telemetry with configurable chaos rates, producing compact Protobuf events to Kafka.
- **Engine (Stream Processor):** Consumes event streams, validates domain invariants, and dispatches records into appropriate sinks (alerts, warnings, or DLQ).
- **Transport & Storage:** Powered by Apache Kafka (KRaft mode) and serialized using Protocol Buffers (`proto3`).

---

## Current Status

The project is under active development. Current progress:

- [x] **Domain Model:** Core telemetry entity (`Event`) and discrete types (`EventType`) established with strict encapsulation.
- [x] **Chaos Generation Logic:** Chaos index algorithm implemented with failure modes (corrupted `SourceID`, irregular timestamps, duplicated/nil event IDs).
- [x] **Schema & Contracts:** Protocol Buffers schema defined (`event.proto`) and compiled to Go.
- [x] **Infrastructure:** Multi-stage Docker packaging and Docker Compose orchestration configured for Kafka and the generator service.
- [ ] **Stream Engine & Workers:** Processing pipeline, consumer groups, and alerting logic currently being built.

---

## Tech Stack

- **Language:** Go (1.25+)
- **Streaming:** Apache Kafka (via `kafka-go`)
- **Serialization:** Protocol Buffers (`proto3`)
- **Containerization:** Docker & Docker Compose (Multi-stage builds)

---

## License

This project is licensed under the [MIT License](LICENSE).
