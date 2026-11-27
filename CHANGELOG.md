<!-- Keep a Changelog guide -> https://keepachangelog.com -->

# Kafka UI Debugger Changelog

## [1.0.1] - 2026-11-26

Full Changelog: https://github.com/EdwardLTC/kafka-ui-debugger/commits/1.0.1

## [Unreleased]

## [1.0.0] - 2026-11-26

### Added

- **Cluster & Topic Management**:
    - Multi-cluster configuration persisted per project (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` with `PLAIN` /
      `SCRAM-SHA-256` / `SCRAM-SHA-512`, custom properties, and connection tester).
    - Topic explorer with search filter, internal topic toggle, topic creation/deletion, partition & replication
      inspection, and non-default topic config viewer.
- **Message Debugger & Visualizer**:
    - Flexible consume modes: `Last N Messages`, `From Earliest`, `From Latest (New Only)`, `From Offset`,
      `From Timestamp`, partition filter, and real-time **Live Tail**.
    - Client-side quick filter across keys, decoded payloads, offsets, and headers.
    - Multi-view message inspector: **Decoded** (pretty JSON), **Tree** (hierarchical field view), **Headers & Key**,
      **Raw UTF-8**, and **Hex Dump**.
    - One-click **Replay in Producer** to pre-fill topic, key, headers, schema type, and payload for quick editing and
      resending.
- **Protobuf & Avro Schema Support**:
    - Load `.proto` source directories directly (no external `protoc` required), compiled `.desc` / `.pb`
      `FileDescriptorSet` files, and `.avsc` Avro schema files or directories.
    - Topic-to-schema pattern mapping (exact or `*` wildcard globs) with automatic decoder selection.
    - **Confluent Schema Registry** wire header stripping and prepending for both Protobuf and Avro.
    - **Schemaless Raw Protobuf Wire Decoder** fallback for inspecting binary Protobuf payloads without a loaded schema.
- **Producer & Example Generator**:
    - Produce messages in `UTF-8 / JSON`, `Protobuf (from JSON)`, `Avro (from JSON)` (with full union/nested record
      support), or `Hex Bytes` with custom headers and partition selection.
    - **Generate Example** button to auto-populate a realistic key and JSON payload from the selected Protobuf or Avro
      schema.
- **Consumer Groups & Lag Debugger**:
    - Inspect consumer group state, coordinator, active members, per-partition lag, and reset offsets (`Earliest` /
      `Latest`).
- **UI & Branding**:
    - Clean IntelliJ New UI-styled tool window with custom plugin icon and light/dark adaptive tool window icons.
