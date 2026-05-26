# Kafka.TestBroker

An in-process fake Kafka broker for integration tests and local development. No Docker, no Zookeeper, no external infrastructure — just add the package and go.

## Installation

```
dotnet add package Kafka.TestBroker
```

## Quick start

```csharp
using Kafka.TestFramework;

var settings = new BrokerSettings
{
    Host = "127.0.0.1",
    Port = 9092,
    Topics = new List<TopicSettings>
    {
        new TopicSettings { Name = "orders",  Partitions = 1 },
        new TopicSettings { Name = "metrics", Partitions = 1 }
    },
    Groups = new List<GroupSettings>
    {
        new GroupSettings { Id = "my-consumer-group" }
    }
};

await using var broker = new TestBroker(settings);
await broker.StartAsync();

Console.WriteLine(broker.BootstrapServers); // "127.0.0.1:9092"
```

Pass `broker.BootstrapServers` to your Confluent.Kafka producer or consumer — they talk to the fake broker exactly as they would to a real one.

## Usage in tests

Use `Port = 0` in tests so the OS picks a free port automatically — this prevents conflicts when tests run in parallel.

```csharp
[Test]
public async Task Order_Is_Processed(CancellationToken cancellationToken)
{
    var settings = new BrokerSettings
    {
        Host = "127.0.0.1",
        Port = 0,   // 0 = random free port, safe for parallel test runs
        Topics = new List<TopicSettings>
        {
            new TopicSettings { Name = "orders", Partitions = 1 }
        },
        Groups = new List<GroupSettings>
        {
            new GroupSettings { Id = "test-group" }
        }
    };

    await using var broker = new TestBroker(settings);
    await broker.StartAsync();

    // Produce
    using var producer = new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = broker.BootstrapServers
    }).Build();

    await producer.ProduceAsync("orders", new Message<string, string>
    {
        Key = "order-1",
        Value = """{ "id": 1, "item": "widget" }"""
    }, cancellationToken);
    producer.Flush(cancellationToken);

    // Consume
    using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = broker.BootstrapServers,
        GroupId           = "test-group",
        AutoOffsetReset   = AutoOffsetReset.Earliest,
        EnableAutoCommit  = false
    }).Build();

    consumer.Subscribe("orders");

    var result = consumer.Consume(TimeSpan.FromSeconds(5));

    Assert.Equal("order-1", result.Message.Key);
    Assert.Contains("widget", result.Message.Value);
}
```

## BrokerSettings reference

| Property | Default | Description |
|---|---|---|
| `Host` | `"localhost"` | IP or hostname the broker binds to |
| `Port` | `0` | TCP port. `0` picks a random free port |
| `Topics` | `[]` | Topics the broker advertises in Metadata responses |
| `Groups` | `[]` | Consumer groups (informational) |

`broker.BootstrapServers` always returns the actual `host:port` string the socket is bound to.

## Multiple topics and partitions

```csharp
var settings = new BrokerSettings
{
    Host = "127.0.0.1",
    Port = 9092,
    Topics = new List<TopicSettings>
    {
        new TopicSettings { Name = "events",  Partitions = 3 },
        new TopicSettings { Name = "metrics", Partitions = 1 },
        new TopicSettings { Name = "orders",  Partitions = 2 }
    },
    Groups = new List<GroupSettings>
    {
        new GroupSettings { Id = "events-group" },
        new GroupSettings { Id = "orders-group" }
    }
};
```

## Error injection

Simulate broker failures to test retry / error handling paths:

```csharp
// Make partition 0 of "orders" return LEADER_NOT_AVAILABLE (error 5)
broker.SetPartitionError("orders", partition: 0, errorCode: 5);

// ... run your code that should handle the error ...

// Restore normal operation
broker.ClearPartitionError("orders", partition: 0);
```

Common error codes:

| Code | Kafka name |
|---|---|
| 3 | `UNKNOWN_TOPIC_OR_PARTITION` |
| 5 | `LEADER_NOT_AVAILABLE` |
| 6 | `NOT_LEADER_FOR_PARTITION` |
| 9 | `REPLICA_NOT_AVAILABLE` |
| 10 | `MESSAGE_TOO_LARGE` |

## Clearing topic data

Reset the message log mid-test — useful for testing "what happens with an empty topic" scenarios or isolating test phases without restarting the broker.

### Clear a single topic

Removes all records from every partition of the topic and resets the partition offsets and committed offsets to zero:

```csharp
broker.ClearTopic("orders");
```

### Clear all topics

Does the same for every topic the broker knows about:

```csharp
broker.ClearAllTopics();
```

### Example: test behaviour after a log reset

```csharp
// Produce some "old" messages
await producer.ProduceAsync("orders", new Message<string, string> { Key = "k1", Value = "old" }, ct);
producer.Flush(ct);

// Wipe the topic — simulates compaction / retention expiry
broker.ClearTopic("orders");

// Produce a fresh message — it lands at offset 0
await producer.ProduceAsync("orders", new Message<string, string> { Key = "k2", Value = "new" }, ct);
producer.Flush(ct);

// A consumer starting from Earliest now sees only the new message
var result = consumer.Consume(TimeSpan.FromSeconds(5));
Assert.Equal("new", result.Message.Value);
Assert.Equal(0, result.Offset.Value);
```

## Clearing consumer group state

Reset committed offsets and rebalance session state so that consumers re-read from the beginning — without touching the message log.

### Clear a single group

```csharp
broker.ClearGroup("my-consumer-group");
```

### Clear all groups

```csharp
broker.ClearAllGroups();
```

## Use in a console app or hosted service

`TestBroker` has no ASP.NET Core dependency — it can run anywhere:

```csharp
// Plain console app
var settings = new BrokerSettings
{
    Host = "127.0.0.1",
    Port = 9092,
    Topics = new List<TopicSettings>
    {
        new TopicSettings { Name = "my-topic", Partitions = 1 }
    }
};

await using var broker = new TestBroker(settings);
await broker.StartAsync();

Console.WriteLine($"Broker listening on {broker.BootstrapServers}");
Console.WriteLine("Press Ctrl+C to stop...");
await Task.Delay(Timeout.Infinite);
```

Or register it in a hosted service:

```csharp
// Program.cs (ASP.NET Core / Generic Host)
builder.Services.AddSingleton<TestBroker>(sp =>
{
    var settings = sp.GetRequiredService<IConfiguration>()
        .GetSection("BrokerSettings")
        .Get<BrokerSettings>() ?? new BrokerSettings();
    return new TestBroker(settings);
});
builder.Services.AddHostedService<BrokerHostedService>();
```

## What the fake broker supports

| Kafka API | Supported |
|---|---|
| ApiVersions | ✅ |
| Metadata | ✅ |
| Produce | ✅ |
| Fetch | ✅ (with `MaxWaitMs` long-poll) |
| OffsetFetch | ✅ |
| OffsetCommit | ✅ |
| ListOffsets | ✅ |
| FindCoordinator | ✅ |
| JoinGroup | ✅ (multi-consumer rebalance) |
| SyncGroup | ✅ |
| Heartbeat | ✅ |
| LeaveGroup | ✅ |
| InitProducerId | ✅ (idempotent producers) |
| GetTelemetrySubscriptions | ✅ |

Tested with `Confluent.Kafka` 2.x.

## License

MIT
