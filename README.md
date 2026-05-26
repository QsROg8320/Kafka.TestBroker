# Kafka Test Broker

A self-contained, in-process Kafka broker implementation for .NET integration testing. Implements the Kafka wire protocol over raw TCP sockets — no real Kafka installation required.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Projects](#projects)
  - [Kafka.Broker](#kafkabroker)
  - [Kafka.TestFramework](#kafkatestframework)
  - [Kafka.Producer](#kafkaproducer)
  - [Kafka.Consumer](#kafkaconsumer)
  - [Kafka.Broker.Tests](#kafkabrokertests)
- [Configuration](#configuration)
- [Supported Kafka API Operations](#supported-kafka-api-operations)
- [State Management](#state-management)
- [Rebalancing Protocol](#rebalancing-protocol)
- [Error Injection](#error-injection)
- [Running the Projects](#running-the-projects)
- [Running Tests](#running-tests)
- [Technology Stack](#technology-stack)

---

## Overview

This repository provides a lightweight, in-memory Kafka broker that speaks the native Kafka binary protocol. It is designed to:

- Replace a real Kafka cluster in integration tests
- Validate producer and consumer behavior without external infrastructure
- Inject partition-level errors to test client fault-tolerance
- Support full consumer group rebalancing and offset tracking

Clients connect using the standard `Confluent.Kafka` library — they cannot distinguish the test broker from a real Kafka cluster.

---

## Architecture

```
+------------------+     Confluent.Kafka      +---------------------+
|  Kafka.Producer  | --------------------------->                   |
+------------------+     TCP :19093           |    Kafka.Broker     |
                                               |    (TestBroker)     |
+------------------+     Confluent.Kafka      |                     |
|  Kafka.Consumer  | --------------------------->                   |
+------------------+                          +----------+----------+
                                                         |
                                              +----------v----------+
                                              |  Kafka.TestFramework|
                                              |  (Socket + Protocol)|
                                              +----------+----------+
                                                         |
                                              +----------v----------+
                                              |   Kafka.Protocol    |
                                              |   (NuGet v8.0.0)    |
                                              +---------------------+
```

**Data flow:**

1. `Kafka.Producer` and `Kafka.Consumer` use `Confluent.Kafka` and connect to the broker via TCP.
2. `Kafka.Broker` (`TestBroker`) handles all incoming connections and routes each request to the appropriate handler.
3. `Kafka.TestFramework` provides the low-level socket server, pipe-based I/O buffering, and the request-subscription mechanism.
4. `Kafka.Protocol` (NuGet) handles binary serialization/deserialization of all Kafka protocol messages.

---

## Project Structure

```
Kafka/
+-- Kafka.sln
+-- global.json
|
+-- Kafka.Broker/                    # Broker executable
|   +-- Program.cs                   # DI setup, IHostedService
|   +-- TestBroker.cs                # Core broker logic (~676 lines)
|   +-- appsettings.json             # Topics, groups, host, port
|
+-- Kafka.TestFramework/             # Reusable networking library
|   +-- KafkaTestFramework.cs        # Abstract base: request routing
|   +-- SocketBasedKafkaTestFramework.cs
|   +-- SocketServer.cs              # TCP accept loop
|   +-- Client.cs                    # Pipe-buffered base client
|   +-- ResponseClient.cs            # Read requests, send responses
|   +-- NetworkStream.cs             # Stream adapter for Kafka.Protocol
|   +-- SocketNetworkClient.cs       # Raw socket wrapper
|
+-- Kafka.Producer/                  # CLI producer application
|   +-- Program.cs
|   +-- appsettings.json
|
+-- Kafka.Consumer/                  # CLI consumer application
|   +-- Program.cs
|   +-- appsettings.json
|
+-- Kafka.Broker.Tests/              # Integration test suite
    +-- BrokerIntegrationTests.cs    # 10 end-to-end tests
    +-- BrokerFixture.cs             # Test broker factory
```

---

## Projects

### Kafka.Broker

The main broker application. It starts a TCP server that implements the Kafka wire protocol and stores all messages in memory.

#### Entry Point — `Program.cs`

Sets up the .NET Generic Host with dependency injection:

```csharp
services.AddSingleton<TestBroker>();
services.AddHostedService<BrokerHostedService>();
```

`BrokerHostedService` implements `IHostedService` and starts/stops the `TestBroker` as the host lifecycle dictates.

#### Core Implementation — `TestBroker.cs`

`TestBroker` inherits from `SocketBasedKafkaTestFramework` and registers a handler for every supported Kafka API request type. It maintains all broker state in concurrent collections.

**In-memory state:**

| Field | Type | Description |
|---|---|---|
| `_recordsByTopicAndPartition` | `ConcurrentDictionary` (topic -> partition -> records) | Message log per topic/partition |
| `_partitionOffsets` | `ConcurrentDictionary<(string, int), long>` | High-water mark per partition |
| `_committedOffsets` | `ConcurrentDictionary<(string, string, int), long>` | Committed offset per (group, topic, partition) |
| `_groupSessions` | `ConcurrentDictionary<string, GroupSession>` | Active rebalance state per consumer group |
| `_syncSessions` | `ConcurrentDictionary<string, SyncSession>` | Synchronization barriers for SyncGroup |
| `_partitionErrors` | `ConcurrentDictionary<(string, int), ErrorCode>` | Injected errors for testing |
| `_nextProducerId` | `long` (Interlocked) | Monotonically increasing producer ID |

**Initialization:** In its constructor, `TestBroker` reads `BrokerSettings` from `IConfiguration`, pre-populates topic/partition maps, and registers all API handlers via the `On<TRequest, TResponse>()` method inherited from `KafkaTestFramework`.

---

### Kafka.TestFramework

A reusable library providing socket networking and the request/response routing substrate.

#### `KafkaTestFramework` (abstract)

Manages subscriptions and the per-client message loop.

```csharp
// Register a synchronous handler
framework.On<ProduceRequest, ProduceResponse>(req => HandleProduce(req));

// Register an async handler
framework.On<FetchRequest, FetchResponse>(async req => await HandleFetch(req));

// Register an async handler with cancellation
framework.On<FetchRequest, FetchResponse>(async (req, ct) => await HandleFetch(req, ct));
```

When a client connects, a background task is started that reads requests, dispatches them to the matching handler by type, and writes the response back.

#### `SocketBasedKafkaTestFramework`

Concrete subclass that binds a TCP port and exposes a `Port` property. Wraps `SocketServer`.

#### `SocketServer`

Runs an accept loop on a background task. Accepted connections are pushed into a `BufferBlock<INetworkClient>` so that multiple callers can dequeue them concurrently without blocking the accept loop.

#### `ResponseClient`

Wraps a network connection with:
- `ReadAsync()` — reads the next `RequestPayload` from the pipe buffer
- `SendAsync()` — serializes and writes a `ResponsePayload`

Uses `System.IO.Pipelines` for zero-copy buffered reads.

#### `SocketNetworkClient`

Thin wrapper around `Socket`. Translates `SocketException` into `OperationCanceledException` so that callers can treat disconnections and cancellation uniformly.

---

### Kafka.Producer

A command-line tool for sending a message from a file to a Kafka topic.

**Usage:**

```
Kafka.Producer produce -t <topic> -k <key> -f <filePath>
```

**Example:**

```
Kafka.Producer produce -t test-topic-1 -k my-key -f ./payload.json
```

The file content is read as bytes and sent as the message value. Topic and key are provided as arguments. Built with [ConsoleAppFramework](https://github.com/Cysharp/ConsoleAppFramework) — arguments are parsed automatically from the method signature.

**Configuration** (`appsettings.json`):

```json
{
  "ProducerSettings": {
    "BootstrapServers": "localhost:19093",
    "GroupId": "test-producer-group",
    "SecurityProtocol": "Plaintext"
  }
}
```

---

### Kafka.Consumer

A command-line tool for consuming messages from a Kafka topic and writing them to a file.

**Usage:**

```
Kafka.Consumer consume -t <topic> -f <outputFilePath>
```

**Example:**

```
Kafka.Consumer consume -t test-topic-1 -f ./output.log
```

Starts consuming from the earliest available offset. Each received message is appended to the output file. Stops cleanly on `Ctrl+C` via `CancellationToken`.

**Configuration** (`appsettings.json`):

```json
{
  "ConsumerSettings": {
    "BootstrapServers": "127.0.0.1:19093",
    "GroupId": "test-consumer-group",
    "ApiVersionRequestTimeoutMs": 30000,
    "SecurityProtocol": "Plaintext"
  }
}
```

---

### Kafka.Broker.Tests

Integration tests that start an in-process broker and communicate with it using real `Confluent.Kafka` clients over a loopback TCP socket.

#### `BrokerFixture`

Factory class with a static `CreateAndStartAsync()` method. Builds an in-memory `IConfiguration` dictionary with topic and group definitions, instantiates `TestBroker`, and calls `StartAsync()`.

#### `BrokerIntegrationTests`

Ten integration tests with explicit timeouts. Each test creates its own broker instance on a dynamic port to avoid inter-test interference.

| Test | What it verifies |
|---|---|
| `Produce_And_Consume_Single_Message` | Basic end-to-end message delivery |
| `Committed_Offsets_Are_Persisted` | Offsets survive consumer restart |
| `Multiple_Consumer_Groups_Independent_Offsets` | Groups track offsets independently |
| `AutoOffsetReset_Earliest_Reads_From_Beginning` | Consumer starts at offset 0 |
| `AutoOffsetReset_Latest_Reads_Only_New_Messages` | Consumer starts at HWM |
| `Headers_ArePreservedEndToEnd` | Message headers are round-tripped intact |
| `MultipleTopics_ConsumerSubscribesToAll_ReceivesFromBoth` | Multi-topic subscription |
| `TwoConsumersInGroup_PartitionsDistributed` | Partition rebalancing between two consumers |
| `IdempotentProducer_DeliversExactlyOnce` | Idempotent producer does not duplicate messages |
| `ErrorInjection_FetchReturnsError` / `ProduceReturnsError` | Client handles injected errors gracefully |

---

## Configuration

All broker configuration lives in `Kafka.Broker/appsettings.json`:

```json
{
  "BrokerSettings": {
    "Host": "127.0.0.1",
    "Port": 19093,
    "Topics": [
      { "Name": "test-topic-1", "Partitions": 1 },
      { "Name": "test-topic-2", "Partitions": 2 }
    ],
    "Groups": [
      { "Id": "test-consumer-group" },
      { "Id": "test-producer-group" }
    ]
  }
}
```

| Setting | Description |
|---|---|
| `Host` | IP address the broker listens on |
| `Port` | TCP port (default `19093`) |
| `Topics[].Name` | Topic name |
| `Topics[].Partitions` | Number of partitions for the topic |
| `Groups[].Id` | Pre-registered consumer group ID |

In tests, configuration is provided programmatically via an `IConfiguration` dictionary, so `appsettings.json` is not required.

---

## Supported Kafka API Operations

| API | Notes |
|---|---|
| `ApiVersions` | Advertises supported request types and version ranges |
| `Metadata` | Returns topic, partition, and broker metadata |
| `Produce` | Appends record batches to in-memory partition logs |
| `Fetch` | Returns records starting from the requested offset |
| `ListOffsets` | Resolves `-2` (earliest/0) and `-1` (latest/HWM) |
| `OffsetFetch` | Returns last committed offset for a consumer group |
| `OffsetCommit` | Persists consumer group offsets |
| `FindCoordinator` | Returns itself as the group coordinator |
| `JoinGroup` | Orchestrates group rebalancing with a 300 ms settle window |
| `SyncGroup` | Distributes partition assignments; followers wait for leader |
| `Heartbeat` | Acknowledges group membership |
| `LeaveGroup` | Removes a member from the group |
| `InitProducerId` | Issues monotonically increasing producer IDs |
| `GetTelemetrySubscriptions` | Returns an empty stub response |

---

## State Management

### Message Storage

Messages are stored in a nested `ConcurrentDictionary`:

```
Topic name -> Partition index -> List<Record>
```

Writes to a partition's record list are protected by a `lock` to prevent concurrent append races. The high-water mark (`_partitionOffsets`) is updated atomically after each append.

### Offset Tracking

Committed offsets are keyed by `(groupId, topicName, partitionIndex)`. `OffsetFetch` returns `-1` (meaning "no committed offset") when the key is absent, which causes clients to fall back to their `AutoOffsetReset` policy.

### Producer IDs

`_nextProducerId` is incremented with `Interlocked.Increment` so concurrent `InitProducerId` requests never receive the same ID.

---

## Rebalancing Protocol

The broker implements a two-phase rebalance:

### Phase 1 — JoinGroup

1. The first member to call `JoinGroup` for a group creates a `GroupSession` with a 300 ms settle timer.
2. Subsequent members arriving within the window are added to the same session.
3. After the timer fires, all pending join futures are resolved together with a new `GenerationId`.
4. The first member to join is elected **leader** and receives the full member list so it can compute assignments.
5. Followers receive an empty assignment list and wait for `SyncGroup`.

### Phase 2 — SyncGroup

1. The leader calls `SyncGroup` with the computed partition assignments.
2. A `SyncSession` acts as a barrier: it holds assignment `TaskCompletionSource`s for all members.
3. When the leader's `SyncGroup` arrives, it completes all waiting tasks with the appropriate per-member assignment.
4. Followers' `SyncGroup` calls return as soon as their `TaskCompletionSource` is resolved.

---

## Error Injection

`TestBroker` exposes two methods for injecting Kafka error codes at the partition level:

```csharp
// Make Fetch and Produce requests for this partition return the given error
broker.SetPartitionError("test-topic-1", partition: 0, ErrorCode.UnknownServerError);

// Remove the injected error
broker.ClearPartitionError("test-topic-1", partition: 0);
```

These are used in integration tests to verify that clients handle transient errors correctly without crashing.

---

## Running the Projects

### Prerequisites

- .NET 8 SDK or later
- No Kafka installation required

### Start the broker

```bash
cd Kafka.Broker
dotnet run
```

The broker starts on `127.0.0.1:19093` (configurable in `appsettings.json`).

### Produce a message

```bash
cd Kafka.Producer
echo "hello world" > payload.txt
dotnet run -- produce -t test-topic-1 -k my-key -f payload.txt
```

### Consume messages

```bash
cd Kafka.Consumer
dotnet run -- consume -t test-topic-1 -f output.log
```

Press `Ctrl+C` to stop the consumer.

---

## Running Tests

```bash
cd Kafka.Broker.Tests
dotnet test
```

Tests use TUnit with built-in parallelization. Each test spins up its own broker instance, so tests run independently and do not share state. All tests have explicit timeouts (30–60 seconds) to fail fast on hangs.

---

## Technology Stack

| Component | Library / Version |
|---|---|
| Target framework | .NET 8.0 (applications), .NET 9.0 (TestFramework) |
| Kafka protocol | `Kafka.Protocol` v8.0.0 |
| Kafka client | `Confluent.Kafka` v2.11.0 |
| CLI framework | `ConsoleAppFramework` v5.5.0 |
| Async I/O | `System.IO.Pipelines`, `System.Threading.Tasks.Dataflow` |
| Test runner | `TUnit` v1.45.29 |
| Assertions | `FluentAssertions` v8.10.0 |
| Hosting | `Microsoft.Extensions.Hosting` |
| Configuration | `Microsoft.Extensions.Configuration` |
