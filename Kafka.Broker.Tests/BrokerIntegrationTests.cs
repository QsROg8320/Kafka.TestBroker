using Confluent.Kafka;
using FluentAssertions;
using System.Collections.Concurrent;

namespace Kafka.Broker.Tests;

public class BrokerIntegrationTests
{
    private static IProducer<string, string> CreateProducer(string bootstrapServers)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            SecurityProtocol = SecurityProtocol.Plaintext,
            MessageTimeoutMs = 10000,
            SocketTimeoutMs = 10000,
        };
        return new ProducerBuilder<string, string>(config).Build();
    }

    private static IConsumer<string, string> CreateConsumer(
        string bootstrapServers,
        string groupId,
        AutoOffsetReset autoOffsetReset)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = autoOffsetReset,
            SecurityProtocol = SecurityProtocol.Plaintext,
            EnableAutoCommit = false,
            SessionTimeoutMs = 6000,
            MaxPollIntervalMs = 30000,
            SocketTimeoutMs = 10000,
            FetchWaitMaxMs = 500,
        };
        return new ConsumerBuilder<string, string>(config).Build();
    }

    [Test]
    [Timeout(30000)]
    public async Task Produce_And_Consume_Single_Message(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("test-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync(
            "test-topic",
            new Message<string, string> { Key = "key1", Value = "hello-world" },
            cts.Token);
        producer.Flush(cts.Token);

        using var consumer = CreateConsumer(broker.BootstrapServers, "group-single", AutoOffsetReset.Earliest);
        consumer.Subscribe("test-topic");

        ConsumeResult<string, string>? consumed = null;
        try
        {
            while (!cts.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                {
                    consumed = result;
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }

        consumed.Should().NotBeNull();
        consumed!.Message.Value.Should().Be("hello-world");
        consumed.TopicPartitionOffset.Offset.Value.Should().Be(0);
    }

    [Test]
    [Timeout(30000)]
    public async Task Committed_Offsets_Are_Persisted(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("persist-topic", groupId: "group-persist");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        // Produce 3 messages
        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("persist-topic", new Message<string, string> { Key = "k1", Value = "msg1" }, cts.Token);
        await producer.ProduceAsync("persist-topic", new Message<string, string> { Key = "k2", Value = "msg2" }, cts.Token);
        await producer.ProduceAsync("persist-topic", new Message<string, string> { Key = "k3", Value = "msg3" }, cts.Token);
        producer.Flush(cts.Token);

        // Consumer 1: consume all 3 and commit after each
        using var consumer1 = CreateConsumer(broker.BootstrapServers, "group-persist", AutoOffsetReset.Earliest);
        consumer1.Subscribe("persist-topic");

        var consumed1 = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && consumed1.Count < 3)
            {
                var result = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                {
                    consumed1.Add(result);
                    consumer1.Commit(result);
                }
            }
        }
        catch (OperationCanceledException) { }

        consumed1.Should().HaveCount(3);
        consumer1.Close();

        // Consumer 2: same group — should start from offset 3 (no messages available)
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "group-persist", AutoOffsetReset.Earliest);
        consumer2.Subscribe("persist-topic");

        var consumed2 = new List<ConsumeResult<string, string>>();
        using var shortCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            while (!shortCts.IsCancellationRequested)
            {
                var result = consumer2.Consume(TimeSpan.FromMilliseconds(300));
                if (result?.Message?.Value != null)
                    consumed2.Add(result);
            }
        }
        catch (OperationCanceledException) { }

        consumed2.Should().BeEmpty("committed offsets should prevent re-reading already-processed messages");
    }

    [Test]
    [Timeout(30000)]
    public async Task Multiple_Consumer_Groups_Independent_Offsets(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("multi-group-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        // Produce 2 messages
        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("multi-group-topic", new Message<string, string> { Key = "k1", Value = "alpha" }, cts.Token);
        await producer.ProduceAsync("multi-group-topic", new Message<string, string> { Key = "k2", Value = "beta" }, cts.Token);
        producer.Flush(cts.Token);

        // Group A — reads both messages
        var messagesA = new List<string>();
        using var consumerA = CreateConsumer(broker.BootstrapServers, "group-A", AutoOffsetReset.Earliest);
        consumerA.Subscribe("multi-group-topic");
        try
        {
            while (!cts.IsCancellationRequested && messagesA.Count < 2)
            {
                var result = consumerA.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                    messagesA.Add(result.Message.Value);
            }
        }
        catch (OperationCanceledException) { }
        consumerA.Close();

        // Group B — independently reads both messages from offset 0
        var messagesB = new List<string>();
        using var consumerB = CreateConsumer(broker.BootstrapServers, "group-B", AutoOffsetReset.Earliest);
        consumerB.Subscribe("multi-group-topic");
        try
        {
            while (!cts.IsCancellationRequested && messagesB.Count < 2)
            {
                var result = consumerB.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                    messagesB.Add(result.Message.Value);
            }
        }
        catch (OperationCanceledException) { }
        consumerB.Close();

        messagesA.Should().BeEquivalentTo(new[] { "alpha", "beta" });
        messagesB.Should().BeEquivalentTo(new[] { "alpha", "beta" });
    }

    [Test]
    [Timeout(30000)]
    public async Task AutoOffsetReset_Earliest_Reads_From_Beginning(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("earliest-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        // Produce BEFORE consumer subscribes
        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("earliest-topic", new Message<string, string> { Key = "k1", Value = "first" }, cts.Token);
        await producer.ProduceAsync("earliest-topic", new Message<string, string> { Key = "k2", Value = "second" }, cts.Token);
        producer.Flush(cts.Token);

        // Consumer with Earliest — should receive both messages
        var received = new List<string>();
        using var consumer = CreateConsumer(broker.BootstrapServers, "group-earliest-2", AutoOffsetReset.Earliest);
        consumer.Subscribe("earliest-topic");
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 2)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                    received.Add(result.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().BeEquivalentTo(new[] { "first", "second" });
    }

    [Test]
    [Timeout(60000)]
    public async Task AutoOffsetReset_Latest_Reads_Only_New_Messages(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("latest-topic");

        // Produce "old-message" before consumer subscribes — it sits at offset 0, HWM=1
        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("latest-topic", new Message<string, string> { Key = "k0", Value = "old-message" }, cancellationToken);
        producer.Flush(cancellationToken);

        // Consumer with Latest subscribes.
        // Calling Consume() in a loop drives librdkafka's protocol loop so it completes
        // JoinGroup/SyncGroup/ListOffsets while HWM is still 1, positioning at offset 1.
        using var consumer = CreateConsumer(broker.BootstrapServers, "group-latest", AutoOffsetReset.Latest);
        consumer.Subscribe("latest-topic");

        using var positioningCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            while (!positioningCts.IsCancellationRequested)
                consumer.Consume(TimeSpan.FromMilliseconds(200));
        }
        catch (OperationCanceledException) { }

        // Consumer is now positioned at offset 1. Produce "new-message" at offset 1.
        await producer.ProduceAsync("latest-topic", new Message<string, string> { Key = "k1", Value = "new-message" }, cancellationToken);
        producer.Flush(cancellationToken);

        // The next Fetch cycle should deliver only "new-message".
        var received = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                    received.Add(result.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle().Which.Should().Be("new-message");
        received.Should().NotContain("old-message");
    }

    [Test]
    [Timeout(30000)]
    public async Task Headers_ArePreservedEndToEnd(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("headers-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        using var producer = CreateProducer(broker.BootstrapServers);
        var message = new Message<string, string>
        {
            Key = "hkey",
            Value = "hvalue",
            Headers = new Headers
            {
                { "correlation-id", System.Text.Encoding.UTF8.GetBytes("abc123") },
                { "source", System.Text.Encoding.UTF8.GetBytes("test") },
            }
        };
        await producer.ProduceAsync("headers-topic", message, cts.Token);
        producer.Flush(cts.Token);

        using var consumer = CreateConsumer(broker.BootstrapServers, "group-headers", AutoOffsetReset.Earliest);
        consumer.Subscribe("headers-topic");

        ConsumeResult<string, string>? consumed = null;
        try
        {
            while (!cts.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                {
                    consumed = result;
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }

        consumed.Should().NotBeNull();
        consumed!.Message.Headers.Should().NotBeNull();
        consumed.Message.Headers.Count.Should().Be(2);

        var correlationHeader = consumed.Message.Headers.FirstOrDefault(h => h.Key == "correlation-id");
        correlationHeader.Should().NotBeNull();
        System.Text.Encoding.UTF8.GetString(correlationHeader!.GetValueBytes()).Should().Be("abc123");

        var sourceHeader = consumed.Message.Headers.FirstOrDefault(h => h.Key == "source");
        sourceHeader.Should().NotBeNull();
        System.Text.Encoding.UTF8.GetString(sourceHeader!.GetValueBytes()).Should().Be("test");
    }

    [Test]
    [Timeout(30000)]
    public async Task MultipleTopics_ConsumerSubscribesToAll_ReceivesFromBoth(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync(
            ("multi-topic-a", 1),
            ("multi-topic-b", 1));
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("multi-topic-a", new Message<string, string> { Key = "k1", Value = "msg-from-a" }, cts.Token);
        await producer.ProduceAsync("multi-topic-b", new Message<string, string> { Key = "k2", Value = "msg-from-b" }, cts.Token);
        producer.Flush(cts.Token);

        using var consumer = CreateConsumer(broker.BootstrapServers, "group-multi-topics", AutoOffsetReset.Earliest);
        consumer.Subscribe(new[] { "multi-topic-a", "multi-topic-b" });

        var received = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 2)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                    received.Add(result.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().BeEquivalentTo(new[] { "msg-from-a", "msg-from-b" });
    }

    [Test]
    [Timeout(60000)]
    public async Task TwoConsumersInGroup_PartitionsDistributed(CancellationToken cancellationToken)
    {
        // Use a 2-partition topic so each consumer can own one partition.
        // Both consumers subscribe before any consume loop runs, so both JoinGroup
        // requests land within the 300ms settling window and are assigned together.
        await using var broker = await BrokerFixture.CreateAndStartAsync("rebalance-topic", partitions: 2);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(45));

        var receivedByC1 = new ConcurrentBag<string>();
        var receivedByC2 = new ConcurrentBag<string>();

        using var consumer1 = CreateConsumer(broker.BootstrapServers, "group-rebalance", AutoOffsetReset.Earliest);
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "group-rebalance", AutoOffsetReset.Earliest);

        // Subscribe both consumers synchronously so both JoinGroup requests go to the broker
        // within the 300ms rebalance window, ensuring they are assigned as a group.
        consumer1.Subscribe("rebalance-topic");
        consumer2.Subscribe("rebalance-topic");

        // Both consumers run in parallel
        var task1 = Task.Run(() =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var result = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                    if (result?.Message?.Value != null)
                        receivedByC1.Add(result.Message.Value);
                }
            }
            catch (OperationCanceledException) { }
        });

        var task2 = Task.Run(() =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var result = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                    if (result?.Message?.Value != null)
                        receivedByC2.Add(result.Message.Value);
                }
            }
            catch (OperationCanceledException) { }
        });

        // Give consumers time to complete JoinGroup/SyncGroup/OffsetFetch
        await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

        // Now produce messages — consumers are already positioned
        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("rebalance-topic", new Message<string, string> { Key = "key0", Value = "msg-p0" }, cts.Token);
        await producer.ProduceAsync("rebalance-topic", new Message<string, string> { Key = "key1", Value = "msg-p1" }, cts.Token);
        producer.Flush(cts.Token);

        // Wait until both consumers together have received all messages
        using var waitCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (!waitCts.IsCancellationRequested)
        {
            if (receivedByC1.Count + receivedByC2.Count >= 2) break;
            await Task.Delay(200, cancellationToken).ConfigureAwait(false);
        }

        cts.Cancel();
        await Task.WhenAll(task1, task2).ConfigureAwait(false);

        var allReceived = receivedByC1.Concat(receivedByC2).ToList();
        allReceived.Should().BeEquivalentTo(new[] { "msg-p0", "msg-p1" },
            "both consumers together should receive all messages from both partitions");
    }

    [Test]
    [Timeout(30000)]
    public async Task IdempotentProducer_DeliversExactlyOnce(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("idempotent-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        var config = new ProducerConfig
        {
            BootstrapServers = broker.BootstrapServers,
            SecurityProtocol = SecurityProtocol.Plaintext,
            EnableIdempotence = true,
            MessageTimeoutMs = 10000,
            SocketTimeoutMs = 10000,
        };
        using var producer = new ProducerBuilder<string, string>(config).Build();

        await producer.ProduceAsync("idempotent-topic", new Message<string, string> { Key = "k1", Value = "idempotent-msg" }, cts.Token);
        producer.Flush(cts.Token);

        using var consumer = CreateConsumer(broker.BootstrapServers, "group-idempotent", AutoOffsetReset.Earliest);
        consumer.Subscribe("idempotent-topic");

        var received = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                    received.Add(result.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle().Which.Should().Be("idempotent-msg");
    }

    [Test]
    [Timeout(30000)]
    public async Task ErrorInjection_FetchReturnsError_ConsumerReceivesError(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("fetch-error-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        // Produce a message first
        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("fetch-error-topic", new Message<string, string> { Key = "k1", Value = "test" }, cts.Token);
        producer.Flush(cts.Token);

        // Inject a LEADER_NOT_AVAILABLE error (error code 5) on fetch
        broker.SetPartitionError("fetch-error-topic", 0, 5);

        var config = new ConsumerConfig
        {
            BootstrapServers = broker.BootstrapServers,
            GroupId = "group-fetch-error",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            SecurityProtocol = SecurityProtocol.Plaintext,
            EnableAutoCommit = false,
            SessionTimeoutMs = 6000,
            SocketTimeoutMs = 10000,
            FetchWaitMaxMs = 500,
        };
        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => { /* swallow */ })
            .Build();
        consumer.Subscribe("fetch-error-topic");

        // With error injected, consumer should not receive the message
        var received = new List<string>();
        using var shortCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            while (!shortCts.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(300));
                if (result?.Message?.Value != null)
                    received.Add(result.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().BeEmpty("partition error should prevent delivery");

        // Clear the error — consumer should now be able to receive
        broker.ClearPartitionError("fetch-error-topic", 0);

        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result?.Message?.Value != null)
                    received.Add(result.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle().Which.Should().Be("test");
    }

    [Test]
    [Timeout(30000)]
    public async Task ErrorInjection_ProduceReturnsError_ProducerThrows(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("produce-error-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        // Inject a MESSAGE_TOO_LARGE error (error code 10) on produce
        broker.SetPartitionError("produce-error-topic", 0, 10);

        using var producer = CreateProducer(broker.BootstrapServers);

        var deliveryException = await FluentActions
            .Awaiting(() => producer.ProduceAsync(
                "produce-error-topic",
                new Message<string, string> { Key = "k1", Value = "should-fail" },
                cts.Token))
            .Should().ThrowAsync<ProduceException<string, string>>();

        deliveryException.Which.Error.Code.Should().NotBe(ErrorCode.NoError);

        // Clear the error — subsequent produce should succeed
        broker.ClearPartitionError("produce-error-topic", 0);

        var result = await producer.ProduceAsync(
            "produce-error-topic",
            new Message<string, string> { Key = "k2", Value = "should-succeed" },
            cts.Token);

        result.Offset.Value.Should().BeGreaterThanOrEqualTo(0);
    }

    [Test]
    [Timeout(30000)]
    public async Task ClearTopic_ResetsMessagesAndOffset(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("clear-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("clear-topic", new Message<string, string> { Key = "k1", Value = "old-1" }, cts.Token);
        await producer.ProduceAsync("clear-topic", new Message<string, string> { Key = "k2", Value = "old-2" }, cts.Token);
        await producer.ProduceAsync("clear-topic", new Message<string, string> { Key = "k3", Value = "old-3" }, cts.Token);
        producer.Flush(cts.Token);

        broker.ClearTopic("clear-topic");

        await producer.ProduceAsync("clear-topic", new Message<string, string> { Key = "k4", Value = "new-1" }, cts.Token);
        producer.Flush(cts.Token);

        using var consumer = CreateConsumer(broker.BootstrapServers, "group-clear-topic", AutoOffsetReset.Earliest);
        consumer.Subscribe("clear-topic");

        var received = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    received.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        // Wait a bit to ensure no extra messages arrive
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null)
                    received.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle("only the message produced after clear should be visible");
        received[0].Message.Value.Should().Be("new-1");
        received[0].TopicPartitionOffset.Offset.Value.Should().Be(0, "offset resets to 0 after clear");
    }

    [Test]
    [Timeout(30000)]
    public async Task ClearTopic_DoesNotAffectOtherTopics(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync(
            ("clear-isolated-a", 1),
            ("clear-isolated-b", 1));
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("clear-isolated-a", new Message<string, string> { Key = "k1", Value = "msg-a" }, cts.Token);
        await producer.ProduceAsync("clear-isolated-b", new Message<string, string> { Key = "k2", Value = "msg-b" }, cts.Token);
        producer.Flush(cts.Token);

        broker.ClearTopic("clear-isolated-a");

        // Topic B must still have its message
        using var consumerB = CreateConsumer(broker.BootstrapServers, "group-isolated-b", AutoOffsetReset.Earliest);
        consumerB.Subscribe("clear-isolated-b");

        var receivedB = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && receivedB.Count < 1)
            {
                var r = consumerB.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    receivedB.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        receivedB.Should().ContainSingle().Which.Should().Be("msg-b", "clearing topic A must not affect topic B");

        // Topic A must be empty
        using var consumerA = CreateConsumer(broker.BootstrapServers, "group-isolated-a", AutoOffsetReset.Earliest);
        consumerA.Subscribe("clear-isolated-a");

        var receivedA = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = consumerA.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null)
                    receivedA.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        receivedA.Should().BeEmpty("topic A was cleared before any consumer connected");
    }

    [Test]
    [Timeout(30000)]
    public async Task ClearAllTopics_ResetsAllTopics(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync(
            ("clear-all-x", 1),
            ("clear-all-y", 1));
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("clear-all-x", new Message<string, string> { Key = "k1", Value = "old-x" }, cts.Token);
        await producer.ProduceAsync("clear-all-y", new Message<string, string> { Key = "k2", Value = "old-y" }, cts.Token);
        producer.Flush(cts.Token);

        broker.ClearAllTopics();

        await producer.ProduceAsync("clear-all-x", new Message<string, string> { Key = "k3", Value = "new-x" }, cts.Token);
        await producer.ProduceAsync("clear-all-y", new Message<string, string> { Key = "k4", Value = "new-y" }, cts.Token);
        producer.Flush(cts.Token);

        // Both topics should contain exactly one message at offset 0
        foreach (var (topic, groupId, expectedValue) in new[]
        {
            ("clear-all-x", "group-clear-all-x", "new-x"),
            ("clear-all-y", "group-clear-all-y", "new-y"),
        })
        {
            using var consumer = CreateConsumer(broker.BootstrapServers, groupId, AutoOffsetReset.Earliest);
            consumer.Subscribe(topic);

            var received = new List<ConsumeResult<string, string>>();
            try
            {
                while (!cts.IsCancellationRequested && received.Count < 1)
                {
                    var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                    if (r?.Message?.Value != null)
                        received.Add(r);
                }
            }
            catch (OperationCanceledException) { }

            using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            try
            {
                while (!drainCts.IsCancellationRequested)
                {
                    var r = consumer.Consume(TimeSpan.FromMilliseconds(200));
                    if (r?.Message?.Value != null)
                        received.Add(r);
                }
            }
            catch (OperationCanceledException) { }

            received.Should().ContainSingle($"only message produced after ClearAllTopics should be in {topic}");
            received[0].Message.Value.Should().Be(expectedValue);
            received[0].TopicPartitionOffset.Offset.Value.Should().Be(0, $"offset in {topic} resets to 0 after ClearAllTopics");

            consumer.Close();
        }
    }

    [Test]
    [Timeout(60000)]
    public async Task ClearTopic_NewConsumerInSameGroup_ReceivesOnlyMessagesAfterClear(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("clear-reread-topic", groupId: "clear-reread-group");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(45));

        using var producer = CreateProducer(broker.BootstrapServers);

        // Phase 1: produce and consume "before" message
        await producer.ProduceAsync("clear-reread-topic", new Message<string, string> { Key = "k1", Value = "before" }, cts.Token);
        producer.Flush(cts.Token);

        using var consumer1 = CreateConsumer(broker.BootstrapServers, "clear-reread-group", AutoOffsetReset.Earliest);
        consumer1.Subscribe("clear-reread-topic");

        ConsumeResult<string, string>? first = null;
        try
        {
            while (!cts.IsCancellationRequested && first == null)
            {
                var r = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    first = r;
            }
        }
        catch (OperationCanceledException) { }

        first.Should().NotBeNull();
        first!.Message.Value.Should().Be("before");
        consumer1.Commit(first);
        consumer1.Close();

        // Phase 2: clear topic data and group session state so consumer2 joins cleanly.
        // ClearGroup resets the broker-side JoinGroup/SyncGroup state; without it the old
        // session.LeaderId from consumer1 lingers and consumer2 never becomes leader,
        // causing SyncGroup to hang waiting for assignments that never arrive.
        broker.ClearTopic("clear-reread-topic");
        broker.ClearGroup("clear-reread-group");

        // Phase 3: produce "after" message — lands at offset 0
        await producer.ProduceAsync("clear-reread-topic", new Message<string, string> { Key = "k2", Value = "after" }, cts.Token);
        producer.Flush(cts.Token);

        // Phase 4: new consumer in same group — committed offset was cleared,
        // so AutoOffsetReset kicks in and it reads from the beginning (offset 0)
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "clear-reread-group", AutoOffsetReset.Earliest);
        consumer2.Subscribe("clear-reread-topic");

        var received = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    received.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        // Drain briefly to make sure no extra messages arrive
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null)
                    received.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle("only the message produced after clear should be visible");
        received[0].Message.Value.Should().Be("after");
        received[0].TopicPartitionOffset.Offset.Value.Should().Be(0, "offset resets to 0 after ClearTopic");
    }

    [Test]
    [Timeout(30000)]
    public async Task ClearTopic_MultiplePartitions_AllPartitionsReset(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("multi-part-clear", partitions: 2);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        using var producer = CreateProducer(broker.BootstrapServers);

        // Produce one message to each partition explicitly
        await producer.ProduceAsync(new TopicPartition("multi-part-clear", new Partition(0)),
            new Message<string, string> { Key = "k0", Value = "old-p0" }, cts.Token);
        await producer.ProduceAsync(new TopicPartition("multi-part-clear", new Partition(1)),
            new Message<string, string> { Key = "k1", Value = "old-p1" }, cts.Token);
        producer.Flush(cts.Token);

        broker.ClearTopic("multi-part-clear");

        // Produce fresh messages to both partitions — should land at offset 0
        await producer.ProduceAsync(new TopicPartition("multi-part-clear", new Partition(0)),
            new Message<string, string> { Key = "k2", Value = "new-p0" }, cts.Token);
        await producer.ProduceAsync(new TopicPartition("multi-part-clear", new Partition(1)),
            new Message<string, string> { Key = "k3", Value = "new-p1" }, cts.Token);
        producer.Flush(cts.Token);

        using var consumer = CreateConsumer(broker.BootstrapServers, "group-multi-part-clear", AutoOffsetReset.Earliest);
        consumer.Subscribe("multi-part-clear");

        var received = new Dictionary<int, ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 2)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    received[r.Partition.Value] = r;
            }
        }
        catch (OperationCanceledException) { }

        received.Should().HaveCount(2, "one message per partition");
        received[0].Message.Value.Should().Be("new-p0");
        received[0].TopicPartitionOffset.Offset.Value.Should().Be(0, "partition 0 offset resets after ClearTopic");
        received[1].Message.Value.Should().Be("new-p1");
        received[1].TopicPartitionOffset.Offset.Value.Should().Be(0, "partition 1 offset resets after ClearTopic");
    }

    [Test]
    [Timeout(60000)]
    public async Task ClearGroup_Alone_ConsumerRereadsExistingMessages(CancellationToken cancellationToken)
    {
        // ClearGroup resets committed offsets and session state but leaves messages intact.
        // A consumer rejoining the same group should re-read from offset 0.
        await using var broker = await BrokerFixture.CreateAndStartAsync("reread-topic", groupId: "reread-group");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(45));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("reread-topic", new Message<string, string> { Key = "k1", Value = "msg-1" }, cts.Token);
        await producer.ProduceAsync("reread-topic", new Message<string, string> { Key = "k2", Value = "msg-2" }, cts.Token);
        producer.Flush(cts.Token);

        // Consumer1: read both messages and commit
        using var consumer1 = CreateConsumer(broker.BootstrapServers, "reread-group", AutoOffsetReset.Earliest);
        consumer1.Subscribe("reread-topic");

        var round1 = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && round1.Count < 2)
            {
                var r = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                {
                    round1.Add(r);
                    consumer1.Commit(r);
                }
            }
        }
        catch (OperationCanceledException) { }

        round1.Should().HaveCount(2);
        consumer1.Close();

        // ClearGroup only — messages in the topic are NOT removed
        broker.ClearGroup("reread-group");

        // Consumer2: same group, committed offset is gone → AutoOffsetReset.Earliest kicks in
        // and the consumer reads the two original messages again
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "reread-group", AutoOffsetReset.Earliest);
        consumer2.Subscribe("reread-topic");

        var round2 = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && round2.Count < 2)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    round2.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        round2.Should().BeEquivalentTo(new[] { "msg-1", "msg-2" },
            "ClearGroup does not erase messages — consumer re-reads from the beginning");
    }

    [Test]
    [Timeout(60000)]
    public async Task ClearAllGroups_ResetsAllGroupOffsets(CancellationToken cancellationToken)
    {
        // After ClearAllGroups both groups lose committed offsets and re-read from the beginning.
        await using var broker = await BrokerFixture.CreateAndStartAsync(
            ("all-groups-topic-a", 1),
            ("all-groups-topic-b", 1));
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(45));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("all-groups-topic-a", new Message<string, string> { Key = "k1", Value = "msg-a" }, cts.Token);
        await producer.ProduceAsync("all-groups-topic-b", new Message<string, string> { Key = "k2", Value = "msg-b" }, cts.Token);
        producer.Flush(cts.Token);

        // Both consumers read and commit their message
        foreach (var (topic, groupId) in new[] { ("all-groups-topic-a", "all-groups-group-a"), ("all-groups-topic-b", "all-groups-group-b") })
        {
            using var c = CreateConsumer(broker.BootstrapServers, groupId, AutoOffsetReset.Earliest);
            c.Subscribe(topic);
            ConsumeResult<string, string>? r = null;
            try { while (!cts.IsCancellationRequested && r?.Message == null) r = c.Consume(TimeSpan.FromMilliseconds(500)); }
            catch (OperationCanceledException) { }
            if (r != null) c.Commit(r);
            c.Close();
        }

        broker.ClearAllGroups();

        // After clearing, both groups should re-read from the beginning
        foreach (var (topic, groupId, expected) in new[]
        {
            ("all-groups-topic-a", "all-groups-group-a", "msg-a"),
            ("all-groups-topic-b", "all-groups-group-b", "msg-b"),
        })
        {
            using var c = CreateConsumer(broker.BootstrapServers, groupId, AutoOffsetReset.Earliest);
            c.Subscribe(topic);
            var received = new List<string>();
            try
            {
                while (!cts.IsCancellationRequested && received.Count < 1)
                {
                    var r = c.Consume(TimeSpan.FromMilliseconds(500));
                    if (r?.Message?.Value != null) received.Add(r.Message.Value);
                }
            }
            catch (OperationCanceledException) { }
            c.Close();

            received.Should().ContainSingle($"group {groupId} should re-read after ClearAllGroups");
            received[0].Should().Be(expected);
        }
    }

    [Test]
    [Timeout(30000)]
    public async Task ClearTopic_DoesNotClearPartitionErrors(CancellationToken cancellationToken)
    {
        // Partition errors are an independent injection mechanism; ClearTopic must not remove them.
        await using var broker = await BrokerFixture.CreateAndStartAsync("error-persist-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("error-persist-topic", new Message<string, string> { Key = "k1", Value = "v1" }, cts.Token);
        producer.Flush(cts.Token);

        broker.SetPartitionError("error-persist-topic", 0, 5); // LEADER_NOT_AVAILABLE — blocks both fetch and produce
        broker.ClearTopic("error-persist-topic");

        // Consumer should NOT receive anything — fetch error is still active after ClearTopic
        var config = new ConsumerConfig
        {
            BootstrapServers = broker.BootstrapServers,
            GroupId = "group-error-persist",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            SecurityProtocol = SecurityProtocol.Plaintext,
            EnableAutoCommit = false,
            SessionTimeoutMs = 6000,
            SocketTimeoutMs = 10000,
            FetchWaitMaxMs = 500,
        };
        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, _) => { })
            .Build();
        consumer.Subscribe("error-persist-topic");

        var receivedWithError = new List<string>();
        using var shortCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            while (!shortCts.IsCancellationRequested)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(300));
                if (r?.Message?.Value != null)
                    receivedWithError.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        receivedWithError.Should().BeEmpty("partition error survives ClearTopic");

        // Clear the error, then produce — now both fetch and produce should work
        broker.ClearPartitionError("error-persist-topic", 0);

        await producer.ProduceAsync("error-persist-topic", new Message<string, string> { Key = "k2", Value = "after-clear" }, cts.Token);
        producer.Flush(cts.Token);

        var receivedAfter = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && receivedAfter.Count < 1)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    receivedAfter.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        receivedAfter.Should().ContainSingle().Which.Should().Be("after-clear");
    }

    [Test]
    [Timeout(30000)]
    public async Task Clear_OnNonExistentOrEmptyResources_DoesNotThrow(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("idempotent-clear-topic");

        // All of these must be safe to call on empty / non-existent resources
        var act = () =>
        {
            broker.ClearTopic("idempotent-clear-topic");   // empty topic
            broker.ClearTopic("does-not-exist");            // unknown topic
            broker.ClearAllTopics();                        // all topics already empty
            broker.ClearGroup("does-not-exist-group");      // unknown group
            broker.ClearTopic("idempotent-clear-topic");   // second clear on same empty topic
        };

        act.Should().NotThrow("clearing empty or unknown resources must be a safe no-op");
    }

    [Test]
    [Timeout(60000)]
    public async Task Consumer_AbruptDisconnect_NewConsumerInSameGroup_ContinuesFromCommittedOffset(CancellationToken cancellationToken)
    {
        // When a consumer exits without sending LeaveGroup (e.g. process crash / Dispose without Close),
        // the broker must re-elect a new leader so the next consumer can complete JoinGroup/SyncGroup.
        await using var broker = await BrokerFixture.CreateAndStartAsync("abrupt-topic", groupId: "abrupt-group");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(45));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("abrupt-topic", new Message<string, string> { Key = "k1", Value = "msg-1" }, cts.Token);
        await producer.ProduceAsync("abrupt-topic", new Message<string, string> { Key = "k2", Value = "msg-2" }, cts.Token);
        await producer.ProduceAsync("abrupt-topic", new Message<string, string> { Key = "k3", Value = "msg-3" }, cts.Token);
        producer.Flush(cts.Token);

        // Consumer1: read msg-1, commit, then Dispose() without Close() — no LeaveGroup is sent
        {
            using var consumer1 = CreateConsumer(broker.BootstrapServers, "abrupt-group", AutoOffsetReset.Earliest);
            consumer1.Subscribe("abrupt-topic");
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var r = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                    if (r?.Message?.Value != null)
                    {
                        consumer1.Commit(r); // committed offset = 1
                        break;
                    }
                }
            }
            catch (OperationCanceledException) { }
            // block exit → Dispose() → no LeaveGroup → session.LeaderId stays as consumer1's id
        }

        // Consumer2: same group — broker re-elects consumer2 as leader during JoinGroup settle
        // Reads from committed offset 1 → only msg-2 and msg-3
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "abrupt-group", AutoOffsetReset.Earliest);
        consumer2.Subscribe("abrupt-topic");

        var received = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 2)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    received.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().BeEquivalentTo(new[] { "msg-2", "msg-3" },
            "new consumer resumes from committed offset even when previous consumer crashed");
    }

    [Test]
    [Timeout(30000)]
    public async Task UncommittedMessages_AreRedelivered_AfterConsumerRestart(CancellationToken cancellationToken)
    {
        // Messages read but not committed must be redelivered to the next consumer in the same group.
        await using var broker = await BrokerFixture.CreateAndStartAsync("redeliver-topic", groupId: "redeliver-group");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("redeliver-topic", new Message<string, string> { Key = "k1", Value = "msg-1" }, cts.Token);
        await producer.ProduceAsync("redeliver-topic", new Message<string, string> { Key = "k2", Value = "msg-2" }, cts.Token);
        await producer.ProduceAsync("redeliver-topic", new Message<string, string> { Key = "k3", Value = "msg-3" }, cts.Token);
        producer.Flush(cts.Token);

        // Consumer1: read all 3 but commit only after msg-1 → committed offset = 1
        using var consumer1 = CreateConsumer(broker.BootstrapServers, "redeliver-group", AutoOffsetReset.Earliest);
        consumer1.Subscribe("redeliver-topic");

        var round1 = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && round1.Count < 3)
            {
                var r = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                {
                    if (round1.Count == 0)
                        consumer1.Commit(r);
                    round1.Add(r);
                }
            }
        }
        catch (OperationCanceledException) { }

        round1.Should().HaveCount(3);
        consumer1.Close();

        // Consumer2: same group — committed offset is 1, so must re-read msg-2 and msg-3
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "redeliver-group", AutoOffsetReset.Earliest);
        consumer2.Subscribe("redeliver-topic");

        var round2 = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && round2.Count < 2)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    round2.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        // Drain briefly to confirm no extra messages
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null)
                    round2.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        round2.Should().BeEquivalentTo(new[] { "msg-2", "msg-3" },
            "uncommitted messages must be redelivered to the next consumer in the group");
    }

    [Test]
    [Timeout(30000)]
    public async Task ProduceMultipleBatches_AllMessagesDeliveredInOrder(CancellationToken cancellationToken)
    {
        // Messages from separate produce calls must all be stored and delivered in order.
        // A subsequent Latest consumer must see nothing (HWM advanced correctly).
        await using var broker = await BrokerFixture.CreateAndStartAsync("batch-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        using var producer = CreateProducer(broker.BootstrapServers);

        var expectedValues = new List<string>();
        for (var i = 0; i < 5; i++)
        {
            var value = $"batch1-msg{i}";
            expectedValues.Add(value);
            await producer.ProduceAsync("batch-topic",
                new Message<string, string> { Key = $"k{i}", Value = value }, cts.Token);
        }
        producer.Flush(cts.Token);

        for (var i = 5; i < 10; i++)
        {
            var value = $"batch2-msg{i}";
            expectedValues.Add(value);
            await producer.ProduceAsync("batch-topic",
                new Message<string, string> { Key = $"k{i}", Value = value }, cts.Token);
        }
        producer.Flush(cts.Token);

        // Consumer1: reads all 10 and commits
        using var consumer1 = CreateConsumer(broker.BootstrapServers, "group-batch", AutoOffsetReset.Earliest);
        consumer1.Subscribe("batch-topic");

        var received = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 10)
            {
                var r = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                {
                    received.Add(r.Message.Value);
                    consumer1.Commit(r);
                }
            }
        }
        catch (OperationCanceledException) { }

        received.Should().HaveCount(10, "all messages from both batches must arrive");
        received.Should().ContainInOrder(expectedValues, "messages must arrive in produce order");
        consumer1.Close();

        // Consumer2 in same group: committed past all 10 messages — nothing to read
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "group-batch", AutoOffsetReset.Earliest);
        consumer2.Subscribe("batch-topic");

        var extra = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null)
                    extra.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        extra.Should().BeEmpty("HWM must advance correctly so the committed consumer sees nothing new");
    }

    [Test]
    [Timeout(30000)]
    public async Task ConcurrentProducers_AllMessagesAreStored(CancellationToken cancellationToken)
    {
        // Two producers writing to the same partition concurrently must not lose or corrupt messages.
        await using var broker = await BrokerFixture.CreateAndStartAsync("concurrent-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        const int messagesPerProducer = 20;

        using var producer1 = CreateProducer(broker.BootstrapServers);
        using var producer2 = CreateProducer(broker.BootstrapServers);

        var task1 = Task.Run(async () =>
        {
            for (var i = 0; i < messagesPerProducer; i++)
                await producer1.ProduceAsync("concurrent-topic",
                    new Message<string, string> { Key = $"p1-{i}", Value = $"producer1-msg{i}" }, cts.Token);
            producer1.Flush(cts.Token);
        }, cts.Token);

        var task2 = Task.Run(async () =>
        {
            for (var i = 0; i < messagesPerProducer; i++)
                await producer2.ProduceAsync("concurrent-topic",
                    new Message<string, string> { Key = $"p2-{i}", Value = $"producer2-msg{i}" }, cts.Token);
            producer2.Flush(cts.Token);
        }, cts.Token);

        await Task.WhenAll(task1, task2);

        using var consumer = CreateConsumer(broker.BootstrapServers, "group-concurrent", AutoOffsetReset.Earliest);
        consumer.Subscribe("concurrent-topic");

        var received = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < messagesPerProducer * 2)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null)
                    received.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().HaveCount(messagesPerProducer * 2, "all messages from both producers must be stored");

        var offsets = received.Select(r => r.Offset.Value).OrderBy(o => o).ToList();
        offsets.Should().BeEquivalentTo(Enumerable.Range(0, messagesPerProducer * 2).Select(i => (long)i),
            "offsets must be unique and sequential — no gaps or duplicates");

        received.Count(r => r.Message.Value.StartsWith("producer1")).Should().Be(messagesPerProducer);
        received.Count(r => r.Message.Value.StartsWith("producer2")).Should().Be(messagesPerProducer);
    }

    [Test]
    [Timeout(60000)]
    public async Task TwoGroupsConcurrentlyFetchSamePartition_CorrectOffsetsForBoth(CancellationToken cancellationToken)
    {
        // Two consumer groups fetching the same single-partition topic at different offsets concurrently.
        // Previously, OffsetDelta was mutated on shared Record objects causing a race condition where
        // one group could corrupt the other group's view of offsets.
        await using var broker = await BrokerFixture.CreateAndStartAsync("concurrent-groups-topic", groupId: "cg-group-a");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(45));

        using var producer = CreateProducer(broker.BootstrapServers);
        const int total = 10;
        for (var i = 0; i < total; i++)
            await producer.ProduceAsync("concurrent-groups-topic",
                new Message<string, string> { Key = $"k{i}", Value = $"msg-{i}" }, cts.Token);
        producer.Flush(cts.Token);

        // Group A reads first half and commits, then continues fetching second half
        using var consumerA1 = CreateConsumer(broker.BootstrapServers, "cg-group-a", AutoOffsetReset.Earliest);
        consumerA1.Subscribe("concurrent-groups-topic");

        var firstHalf = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && firstHalf.Count < 5)
            {
                var r = consumerA1.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) { firstHalf.Add(r.Message.Value); consumerA1.Commit(r); }
            }
        }
        catch (OperationCanceledException) { }
        consumerA1.Close();

        // Group A consumer 2 starts at committed offset 5 (second half)
        using var consumerA2 = CreateConsumer(broker.BootstrapServers, "cg-group-a", AutoOffsetReset.Earliest);
        consumerA2.Subscribe("concurrent-groups-topic");

        // Group B starts from offset 0 (all 10 messages) — runs concurrently with Group A reading offset 5+
        using var consumerB = CreateConsumer(broker.BootstrapServers, "cg-group-b", AutoOffsetReset.Earliest);
        consumerB.Subscribe("concurrent-groups-topic");

        var secondHalf = new ConcurrentBag<string>();
        var allFromB = new ConcurrentBag<string>();

        var taskA = Task.Run(async () =>
        {
            try
            {
                while (!cts.IsCancellationRequested && secondHalf.Count < 5)
                {
                    var r = consumerA2.Consume(TimeSpan.FromMilliseconds(500));
                    if (r?.Message?.Value != null) secondHalf.Add(r.Message.Value);
                }
            }
            catch (OperationCanceledException) { }
            await Task.CompletedTask;
        }, cts.Token);

        var taskB = Task.Run(async () =>
        {
            try
            {
                while (!cts.IsCancellationRequested && allFromB.Count < total)
                {
                    var r = consumerB.Consume(TimeSpan.FromMilliseconds(500));
                    if (r?.Message?.Value != null) allFromB.Add(r.Message.Value);
                }
            }
            catch (OperationCanceledException) { }
            await Task.CompletedTask;
        }, cts.Token);

        await Task.WhenAll(taskA, taskB);

        firstHalf.Should().HaveCount(5).And.ContainInOrder(
            Enumerable.Range(0, 5).Select(i => $"msg-{i}"));

        secondHalf.Should().HaveCount(5);
        secondHalf.OrderBy(v => v).Should().BeEquivalentTo(
            Enumerable.Range(5, 5).Select(i => $"msg-{i}"),
            "group A second consumer must see only messages 5-9");

        allFromB.Should().HaveCount(total);
        allFromB.OrderBy(v => v).Should().BeEquivalentTo(
            Enumerable.Range(0, total).Select(i => $"msg-{i}"),
            "group B must see all 10 messages regardless of group A's concurrent fetch");
    }

    [Test]
    [Timeout(30000)]
    public async Task ProducerBatchedMessages_OffsetsDerivedCorrectly(CancellationToken cancellationToken)
    {
        // When librdkafka batches multiple records into one ProduceRequest (LingerMs > 0),
        // each record arrives with OffsetDelta 0,1,2... relative to the batch.
        // The broker must stamp correct absolute offsets so consumers see sequential offsets.
        await using var broker = await BrokerFixture.CreateAndStartAsync("batch-offset-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        // Use LingerMs to encourage librdkafka to batch records into one ProduceRequest
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = broker.BootstrapServers,
            SecurityProtocol = SecurityProtocol.Plaintext,
            LingerMs = 50,
            MessageTimeoutMs = 10000,
        }).Build();

        const int count = 8;
        var tasks = Enumerable.Range(0, count)
            .Select(i => producer.ProduceAsync("batch-offset-topic",
                new Message<string, string> { Key = $"k{i}", Value = $"v{i}" }, cts.Token))
            .ToList();
        await Task.WhenAll(tasks);
        producer.Flush(cts.Token);

        using var consumer = CreateConsumer(broker.BootstrapServers, "group-batch-offset", AutoOffsetReset.Earliest);
        consumer.Subscribe("batch-offset-topic");

        var received = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < count)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) received.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().HaveCount(count);

        // Offsets must be 0..count-1 regardless of how librdkafka batched them
        var offsets = received.Select(r => r.Offset.Value).OrderBy(o => o).ToList();
        offsets.Should().BeEquivalentTo(Enumerable.Range(0, count).Select(i => (long)i),
            "each message must have a unique sequential absolute offset");
    }

    [Test]
    [Timeout(30000)]
    public async Task ConsumerOffsets_AreCorrectAfterPartialRead(CancellationToken cancellationToken)
    {
        // Verifies that ConsumeResult.Offset is correct for every message so that
        // Commit(result) advances the group's committed offset precisely.
        await using var broker = await BrokerFixture.CreateAndStartAsync("partial-read-topic", groupId: "partial-group");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        using var producer = CreateProducer(broker.BootstrapServers);
        for (var i = 0; i < 6; i++)
            await producer.ProduceAsync("partial-read-topic",
                new Message<string, string> { Key = $"k{i}", Value = $"msg-{i}" }, cts.Token);
        producer.Flush(cts.Token);

        // Consumer 1: reads and commits only first 3 messages
        using var consumer1 = CreateConsumer(broker.BootstrapServers, "partial-group", AutoOffsetReset.Earliest);
        consumer1.Subscribe("partial-read-topic");

        var batch1 = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && batch1.Count < 3)
            {
                var r = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) { batch1.Add(r); consumer1.Commit(r); }
            }
        }
        catch (OperationCanceledException) { }

        batch1.Select(r => r.Offset.Value).Should().BeEquivalentTo(new long[] { 0, 1, 2 },
            "first 3 messages must have offsets 0, 1, 2");
        consumer1.Close();

        // Consumer 2: same group — committed offset = 3, must read only messages 3,4,5
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "partial-group", AutoOffsetReset.Earliest);
        consumer2.Subscribe("partial-read-topic");

        var batch2 = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && batch2.Count < 3)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) batch2.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        // Drain to confirm exactly 3
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) batch2.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        batch2.Should().HaveCount(3, "exactly 3 uncommitted messages remain");
        batch2.Select(r => r.Offset.Value).Should().BeEquivalentTo(new long[] { 3, 4, 5 },
            "consumer 2 must start exactly at committed offset 3");
        batch2.Select(r => r.Message.Value).Should().BeEquivalentTo(
            new[] { "msg-3", "msg-4", "msg-5" });
    }
}
