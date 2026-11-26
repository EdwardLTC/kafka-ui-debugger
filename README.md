# Kafka UI Debugger

An IntelliJ IDEA plugin for debugging Apache Kafka directly inside your IDE, with first-class **Protobuf** (`.proto`, `.desc`/`.pb`) and **Apache Avro** (`.avsc`) message visualization, schemaless wire decoding, live tailing, message replay, and consumer group lag inspection.

## Features

### 1. Cluster & Topic Management
- **Multi-cluster support**: Add, edit, remove, and persist Kafka cluster configurations per project (`kafkaUiDebugger.xml`).
- **Security protocols**: Supports `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`), custom truststores, extra Kafka properties, and built-in **Test Connection**.
- **Topic Explorer**: Search/filter topics, toggle internal topics, create or delete topics, and inspect partition leaders, replicas, ISR, offsets, and non-default topic configs.

### 2. Message Debugger & Visualizer
- **Consume Modes**: `Last N Messages`, `From Earliest`, `From Latest (New Only)`, `From Offset`, `From Timestamp`, partition filtering, and real-time **Live Tail**.
- **Client-side Filter**: Instantly search across message keys, decoded JSON payloads, offsets, and headers.
- **5 Detail Views**:
  - **Decoded**: Pretty-printed JSON with decoder badge and **Copy JSON**.
  - **Tree**: Interactive hierarchical tree showing field numbers/names, types, and nested records/arrays.
  - **Headers & Key**: Full key text and headers table (UTF-8 & Hex).
  - **Raw**: Raw UTF-8 payload.
  - **Hex**: Formatted hex + ASCII dump.
- **One-Click Replay**: Click **Replay in Producer ->** on any consumed message to pre-fill the Producer tab with the topic, key, headers, schema type, and decoded JSON payload.

### 3. Protobuf & Avro Support
- **Zero external `protoc` required**: Load `.proto` source directories directly (powered by Wire Schema), compiled `.desc` / `.pb` `FileDescriptorSet` files, or `.avsc` Avro schema files/directories.
- **Topic-to-Schema Mapping**: Map exact topic names or wildcard globs (e.g. `orders.*.events`) to a Protobuf/Avro message type so the right decoder is selected automatically.
- **Confluent Schema Registry Wire Format**: Strip or prepend Confluent wire headers (`0x00` + 4-byte schema ID + message indexes for Protobuf, or `0x00` + 4-byte schema ID for Avro).
- **Schemaless Raw Protobuf Wire Decoder**: Automatically decodes binary Protobuf payloads into a readable JSON/Tree view by field number even when no `.proto` schema is loaded.

### 4. Producer & Schema Example Generator
- **Formats**: Send `UTF-8 / JSON`, `Protobuf (from JSON)`, `Avro (from JSON)` (with full support for Avro unions, nested records, arrays, maps, and enums), or `Hex Bytes`.
- **Generate Example**: Click **Generate Example** to auto-populate a realistic key and JSON payload from the selected Protobuf or Avro schema.
- **Custom Partition, Key & Headers**: Full control over target partition, message key, and record headers.

### 5. Consumer Groups & Lag Debugger
- Inspect consumer group state, coordinator, active members, and per-partition committed offset, log end offset, and lag.
- **Reset Offsets** to `Earliest` or `Latest` for inactive groups.

---

## Quick Start

1. Open the **Kafka Debugger** tool window at the bottom of IntelliJ IDEA.
2. Select or add a cluster via **`+`** in the top toolbar and click **Refresh**.
3. Click **Schemas (Proto/Avro)...** to add your `.proto` directories, `.desc`/`.pb` files, or `.avsc` schemas/directories, and optionally configure **Topic -> Schema** mappings.
4. Double-click any topic in the left explorer to start consuming, inspecting, or producing messages.

---

## Building & Installing

### Build & Run Tests
```bash
./gradlew test buildPlugin
```

### Run in a Sandbox IntelliJ IDEA
```bash
./gradlew runIde
```

### Install from Disk
1. Build the plugin distribution: `./gradlew buildPlugin`
2. In IntelliJ IDEA, open **Settings -> Plugins -> Gear Icon -> Install Plugin from Disk...**
3. Select `build/distributions/kafka-ui-debugger-1.0.0-SNAPSHOT.zip` and restart the IDE.

---

## Project Structure

```text
src/
├── main/
│   ├── kotlin/dev/edward/kafkadebugger/
│   │   ├── kafka/   # Kafka Admin, Consumer, Producer client service
│   │   ├── model/   # Cluster, Topic, Message, Schema & Group models
│   │   ├── proto/   # Protobuf/Avro registry, Confluent wire, Raw decoder, Example generator
│   │   ├── state/   # PersistentStateComponent (kafkaUiDebugger.xml)
│   │   └── ui/      # ToolWindow, Consumer, Producer, Topic/Group & Settings dialogs
│   └── resources/
│       ├── META-INF/ # plugin.xml & pluginIcon.svg
│       └── icons/    # Light & dark ToolWindow icons
└── test/
    └── kotlin/dev/edward/kafkadebugger/
        └── ProtoDebuggerTest.kt
```
