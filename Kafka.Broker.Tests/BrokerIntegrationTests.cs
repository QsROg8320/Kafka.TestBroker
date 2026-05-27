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

        using var positioningCts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
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

    [Test]
    [Timeout(60000)]
    public async Task Latest_SubscribesAfterClear_ThenProduce_GetsNewMessage(CancellationToken cancellationToken)
    {
        // After ClearTopic the HWM resets to 0. A Latest consumer that subscribes
        // before any new produce lands at offset 0 and should receive the first
        // message produced after the clear.
        await using var broker = await BrokerFixture.CreateAndStartAsync("latest-after-clear-topic");

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("latest-after-clear-topic",
            new Message<string, string> { Key = "k0", Value = "old-message" }, cancellationToken);
        producer.Flush(cancellationToken);

        broker.ClearTopic("latest-after-clear-topic");

        // Subscribe after clear (HWM=0). Drive protocol loop so JoinGroup/SyncGroup/ListOffsets complete.
        using var consumer = CreateConsumer(broker.BootstrapServers, "group-latest-after-clear", AutoOffsetReset.Latest);
        consumer.Subscribe("latest-after-clear-topic");

        using var positioningCts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        try { while (!positioningCts.IsCancellationRequested) consumer.Consume(TimeSpan.FromMilliseconds(200)); }
        catch (OperationCanceledException) { }

        await producer.ProduceAsync("latest-after-clear-topic",
            new Message<string, string> { Key = "k1", Value = "new-message" }, cancellationToken);
        producer.Flush(cancellationToken);

        var received = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) received.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle().Which.Should().Be("new-message");
        received.Should().NotContain("old-message", "old-message was removed by ClearTopic");
    }

    [Test]
    [Timeout(60000)]
    public async Task Latest_SubscribesAfterClearAndProduce_MissesExistingMessage_GetsSubsequentMessage(CancellationToken cancellationToken)
    {
        // Clear + produce "after-clear" (HWM=1). A Late Latest subscriber positions at
        // HWM=1 and must NOT see "after-clear". It should receive only the next produce.
        await using var broker = await BrokerFixture.CreateAndStartAsync("latest-late-subscribe-topic");

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("latest-late-subscribe-topic",
            new Message<string, string> { Key = "k0", Value = "pre-clear" }, cancellationToken);
        producer.Flush(cancellationToken);

        broker.ClearTopic("latest-late-subscribe-topic");

        await producer.ProduceAsync("latest-late-subscribe-topic",
            new Message<string, string> { Key = "k1", Value = "after-clear" }, cancellationToken);
        producer.Flush(cancellationToken);

        // Subscribe with Latest at HWM=1 — misses "after-clear"
        using var consumer = CreateConsumer(broker.BootstrapServers, "group-latest-late-sub", AutoOffsetReset.Latest);
        consumer.Subscribe("latest-late-subscribe-topic");

        using var positioningCts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        try { while (!positioningCts.IsCancellationRequested) consumer.Consume(TimeSpan.FromMilliseconds(200)); }
        catch (OperationCanceledException) { }

        await producer.ProduceAsync("latest-late-subscribe-topic",
            new Message<string, string> { Key = "k2", Value = "newest" }, cancellationToken);
        producer.Flush(cancellationToken);

        var received = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) received.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle().Which.Should().Be("newest");
        received.Should().NotContain("after-clear");
        received.Should().NotContain("pre-clear");
    }

    [Test]
    [Timeout(30000)]
    public async Task Earliest_AfterMultipleClears_AlwaysStartsFromOffset0(CancellationToken cancellationToken)
    {
        // Each ClearTopic resets the HWM to 0. After N clear cycles Earliest must see
        // only the messages produced after the last clear, starting at offset 0.
        await using var broker = await BrokerFixture.CreateAndStartAsync("multi-clear-earliest-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        using var producer = CreateProducer(broker.BootstrapServers);

        await producer.ProduceAsync("multi-clear-earliest-topic",
            new Message<string, string> { Key = "k1", Value = "round1" }, cts.Token);
        producer.Flush(cts.Token);
        broker.ClearTopic("multi-clear-earliest-topic");

        await producer.ProduceAsync("multi-clear-earliest-topic",
            new Message<string, string> { Key = "k2", Value = "round2" }, cts.Token);
        producer.Flush(cts.Token);
        broker.ClearTopic("multi-clear-earliest-topic");

        await producer.ProduceAsync("multi-clear-earliest-topic",
            new Message<string, string> { Key = "k3", Value = "final" }, cts.Token);
        producer.Flush(cts.Token);

        using var consumer = CreateConsumer(broker.BootstrapServers, "group-multi-clear-earliest", AutoOffsetReset.Earliest);
        consumer.Subscribe("multi-clear-earliest-topic");

        var received = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) received.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) received.Add(r);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle("only the post-last-clear message survives");
        received[0].Message.Value.Should().Be("final");
        received[0].TopicPartitionOffset.Offset.Value.Should().Be(0, "HWM resets to 0 after each ClearTopic");
    }

    [Test]
    [Timeout(60000)]
    public async Task Latest_AfterMultipleClears_PositionsAtCurrentHWM(CancellationToken cancellationToken)
    {
        // After repeated clear+produce cycles the surviving HWM is 1 (one message since last clear).
        // A Latest subscriber must position at that HWM and miss all pre-subscription messages.
        await using var broker = await BrokerFixture.CreateAndStartAsync("multi-clear-latest-topic");

        using var producer = CreateProducer(broker.BootstrapServers);

        await producer.ProduceAsync("multi-clear-latest-topic",
            new Message<string, string> { Key = "k1", Value = "round1" }, cancellationToken);
        producer.Flush(cancellationToken);
        broker.ClearTopic("multi-clear-latest-topic");

        await producer.ProduceAsync("multi-clear-latest-topic",
            new Message<string, string> { Key = "k2", Value = "round2" }, cancellationToken);
        producer.Flush(cancellationToken);
        broker.ClearTopic("multi-clear-latest-topic");

        // After last clear: HWM=0. Produce one more message → HWM=1.
        await producer.ProduceAsync("multi-clear-latest-topic",
            new Message<string, string> { Key = "k3", Value = "after-last-clear" }, cancellationToken);
        producer.Flush(cancellationToken);

        // Latest subscriber positions at HWM=1 — misses "after-last-clear"
        using var consumer = CreateConsumer(broker.BootstrapServers, "group-multi-clear-latest", AutoOffsetReset.Latest);
        consumer.Subscribe("multi-clear-latest-topic");

        using var positioningCts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        try { while (!positioningCts.IsCancellationRequested) consumer.Consume(TimeSpan.FromMilliseconds(200)); }
        catch (OperationCanceledException) { }

        await producer.ProduceAsync("multi-clear-latest-topic",
            new Message<string, string> { Key = "k4", Value = "new-message" }, cancellationToken);
        producer.Flush(cancellationToken);

        var received = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) received.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle().Which.Should().Be("new-message");
        received.Should().NotContain("after-last-clear", "Latest positioned past it at subscription time");
    }

    [Test]
    [Timeout(60000)]
    public async Task ClearThenProduce_EarliestGetsAllMessages_LatestPositionsAtHWM(CancellationToken cancellationToken)
    {
        // Same topic, two groups, different auto.offset.reset.
        // Earliest reads all messages produced after clear; Latest sees nothing
        // until a new message arrives after it has subscribed.
        await using var broker = await BrokerFixture.CreateAndStartAsync("offset-reset-comparison-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(45));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("offset-reset-comparison-topic",
            new Message<string, string> { Key = "k0", Value = "before-clear" }, cancellationToken);
        producer.Flush(cancellationToken);

        broker.ClearTopic("offset-reset-comparison-topic");

        await producer.ProduceAsync("offset-reset-comparison-topic",
            new Message<string, string> { Key = "k1", Value = "msg-1" }, cancellationToken);
        await producer.ProduceAsync("offset-reset-comparison-topic",
            new Message<string, string> { Key = "k2", Value = "msg-2" }, cancellationToken);
        producer.Flush(cancellationToken);

        // Latest consumer subscribes at HWM=2
        using var latestConsumer = CreateConsumer(broker.BootstrapServers, "group-latest-compare", AutoOffsetReset.Latest);
        latestConsumer.Subscribe("offset-reset-comparison-topic");

        using var positioningCts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        try { while (!positioningCts.IsCancellationRequested) latestConsumer.Consume(TimeSpan.FromMilliseconds(200)); }
        catch (OperationCanceledException) { }

        // Earliest consumer reads both post-clear messages
        using var earliestConsumer = CreateConsumer(broker.BootstrapServers, "group-earliest-compare", AutoOffsetReset.Earliest);
        earliestConsumer.Subscribe("offset-reset-comparison-topic");

        var earliestReceived = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && earliestReceived.Count < 2)
            {
                var r = earliestConsumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) earliestReceived.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        earliestReceived.Should().BeEquivalentTo(new[] { "msg-1", "msg-2" });
        earliestReceived.Should().NotContain("before-clear", "ClearTopic removed it");

        // Latest should have received nothing so far
        var latestReceived = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = latestConsumer.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) latestReceived.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        latestReceived.Should().BeEmpty("Latest positioned at HWM sees no pre-subscription messages");

        // Produce one more — Latest must now receive it
        await producer.ProduceAsync("offset-reset-comparison-topic",
            new Message<string, string> { Key = "k3", Value = "msg-3" }, cancellationToken);
        producer.Flush(cancellationToken);

        try
        {
            while (!cts.IsCancellationRequested && latestReceived.Count < 1)
            {
                var r = latestConsumer.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) latestReceived.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        latestReceived.Should().ContainSingle().Which.Should().Be("msg-3");
    }

    [Test]
    [Timeout(60000)]
    public async Task ClearAllTopics_Latest_ConsumerGetsOnlySubsequentMessages(CancellationToken cancellationToken)
    {
        // ClearAllTopics resets HWM to 0 on every topic. Latest consumers that subscribe
        // after the clear and before new produces must receive only the new messages.
        await using var broker = await BrokerFixture.CreateAndStartAsync(
            ("clear-all-latest-a", 1),
            ("clear-all-latest-b", 1));

        using var producer = CreateProducer(broker.BootstrapServers);

        await producer.ProduceAsync("clear-all-latest-a",
            new Message<string, string> { Key = "k1", Value = "old-a" }, cancellationToken);
        await producer.ProduceAsync("clear-all-latest-b",
            new Message<string, string> { Key = "k2", Value = "old-b" }, cancellationToken);
        producer.Flush(cancellationToken);

        broker.ClearAllTopics();

        // Both Latest consumers subscribe with HWM=0 on both topics
        using var consumerA = CreateConsumer(broker.BootstrapServers, "group-latest-all-a", AutoOffsetReset.Latest);
        using var consumerB = CreateConsumer(broker.BootstrapServers, "group-latest-all-b", AutoOffsetReset.Latest);
        consumerA.Subscribe("clear-all-latest-a");
        consumerB.Subscribe("clear-all-latest-b");

        using var positioningCts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var posA = Task.Run(() =>
        {
            try { while (!positioningCts.IsCancellationRequested) consumerA.Consume(TimeSpan.FromMilliseconds(200)); }
            catch (OperationCanceledException) { }
        });
        var posB = Task.Run(() =>
        {
            try { while (!positioningCts.IsCancellationRequested) consumerB.Consume(TimeSpan.FromMilliseconds(200)); }
            catch (OperationCanceledException) { }
        });
        await Task.WhenAll(posA, posB);

        await producer.ProduceAsync("clear-all-latest-a",
            new Message<string, string> { Key = "k3", Value = "new-a" }, cancellationToken);
        await producer.ProduceAsync("clear-all-latest-b",
            new Message<string, string> { Key = "k4", Value = "new-b" }, cancellationToken);
        producer.Flush(cancellationToken);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var receivedA = new List<string>();
        var receivedB = new List<string>();

        try
        {
            while (!cts.IsCancellationRequested && receivedA.Count < 1)
            {
                var r = consumerA.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) receivedA.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        try
        {
            while (!cts.IsCancellationRequested && receivedB.Count < 1)
            {
                var r = consumerB.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) receivedB.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        receivedA.Should().ContainSingle().Which.Should().Be("new-a");
        receivedB.Should().ContainSingle().Which.Should().Be("new-b");
    }

    [Test]
    [Timeout(60000)]
    public async Task ClearGroup_Latest_NewConsumerPositionsAtHWM_NotAtBeginning(CancellationToken cancellationToken)
    {
        // ClearGroup wipes committed offsets. A replacement Latest consumer uses
        // ListOffsets(Latest) = HWM and does NOT re-read old messages —
        // unlike Earliest which would reset to offset 0 (tested in ClearGroup_Alone test).
        await using var broker = await BrokerFixture.CreateAndStartAsync("clear-group-latest-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(45));

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("clear-group-latest-topic",
            new Message<string, string> { Key = "k1", Value = "existing-1" }, cts.Token);
        await producer.ProduceAsync("clear-group-latest-topic",
            new Message<string, string> { Key = "k2", Value = "existing-2" }, cts.Token);
        producer.Flush(cts.Token);

        // Consumer1: read and commit both messages, then close cleanly
        using var consumer1 = CreateConsumer(broker.BootstrapServers, "group-cg-latest", AutoOffsetReset.Earliest);
        consumer1.Subscribe("clear-group-latest-topic");

        var round1 = new List<ConsumeResult<string, string>>();
        try
        {
            while (!cts.IsCancellationRequested && round1.Count < 2)
            {
                var r = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) { round1.Add(r); consumer1.Commit(r); }
            }
        }
        catch (OperationCanceledException) { }

        round1.Should().HaveCount(2);
        consumer1.Close();

        // Reset group state — committed offsets gone, group session cleared
        broker.ClearGroup("group-cg-latest");

        // Consumer2 with Latest: no committed offset → auto.offset.reset=Latest → HWM=2
        // Must NOT re-read existing-1 or existing-2
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "group-cg-latest", AutoOffsetReset.Latest);
        consumer2.Subscribe("clear-group-latest-topic");

        using var positioningCts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        try { while (!positioningCts.IsCancellationRequested) consumer2.Consume(TimeSpan.FromMilliseconds(200)); }
        catch (OperationCanceledException) { }

        var receivedOld = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) receivedOld.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        receivedOld.Should().BeEmpty("Latest after ClearGroup positions at HWM, not at offset 0");

        // Produce a new message — consumer2 must receive it
        await producer.ProduceAsync("clear-group-latest-topic",
            new Message<string, string> { Key = "k3", Value = "new-message" }, cts.Token);
        producer.Flush(cts.Token);

        var receivedNew = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && receivedNew.Count < 1)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) receivedNew.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        receivedNew.Should().ContainSingle().Which.Should().Be("new-message");
    }

    [Test]
    [Timeout(30000)]
    public async Task AfterClearGroupAndTopic_NewProducer_NewEarliestConsumer_ReceivesMessages(CancellationToken cancellationToken)
    {
        await using var broker = await BrokerFixture.CreateAndStartAsync("reset-full-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        // Phase 1: produce and consume
        using var producer1 = CreateProducer(broker.BootstrapServers);
        await producer1.ProduceAsync("reset-full-topic",
            new Message<string, string> { Key = "k1", Value = "before-clear" }, cts.Token);
        producer1.Flush(cts.Token);

        using var consumer1 = CreateConsumer(broker.BootstrapServers, "reset-full-group", AutoOffsetReset.Earliest);
        consumer1.Subscribe("reset-full-topic");

        ConsumeResult<string, string>? first = null;
        try
        {
            while (!cts.IsCancellationRequested && first == null)
            {
                var r = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) { first = r; consumer1.Commit(r); }
            }
        }
        catch (OperationCanceledException) { }

        first.Should().NotBeNull();
        consumer1.Close();

        // Phase 2: clear group and topic
        broker.ClearGroup("reset-full-group");
        broker.ClearTopic("reset-full-topic");

        // Phase 3: new producer sends a message
        using var producer2 = CreateProducer(broker.BootstrapServers);
        await producer2.ProduceAsync("reset-full-topic",
            new Message<string, string> { Key = "k2", Value = "after-clear" }, cts.Token);
        producer2.Flush(cts.Token);

        // Phase 4: new Earliest consumer in same group — must receive "after-clear"
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "reset-full-group", AutoOffsetReset.Earliest);
        consumer2.Subscribe("reset-full-topic");

        var received = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) received.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle().Which.Should().Be("after-clear");
    }

    [Test]
    [Timeout(30000)]
    public async Task AfterClearGroupAndTopic_ActiveConsumer1NotClosed_NewEarliestConsumer_ReceivesMessages(CancellationToken cancellationToken)
    {
        // Reproduces the bug: consumer1 is still actively polling (not closed) when
        // ClearGroup+ClearTopic is called. Consumer1 and consumer2 both end up in the
        // same SettleJoinGroup window. With 1 partition and 2 consumers the leader
        // assigns the partition to one of them — consumer2 gets nothing if consumer1
        // wins the assignment.
        await using var broker = await BrokerFixture.CreateAndStartAsync("live-consumer-clear-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        using var producer1 = CreateProducer(broker.BootstrapServers);
        await producer1.ProduceAsync("live-consumer-clear-topic",
            new Message<string, string> { Key = "k1", Value = "before-clear" }, cts.Token);
        producer1.Flush(cts.Token);

        // consumer1 polls in the background and is intentionally NOT closed before the clear
        var consumer1 = CreateConsumer(broker.BootstrapServers, "live-group", AutoOffsetReset.Earliest);
        consumer1.Subscribe("live-consumer-clear-topic");

        using var consumer1Cts = new CancellationTokenSource();
        var consumer1Read = new TaskCompletionSource<bool>();
        var consumer1Task = Task.Run(() =>
        {
            try
            {
                while (!consumer1Cts.IsCancellationRequested)
                {
                    var r = consumer1.Consume(TimeSpan.FromMilliseconds(200));
                    if (r?.Message?.Value == "before-clear")
                        consumer1Read.TrySetResult(true);
                }
            }
            catch (OperationCanceledException) { }
        }, cancellationToken);

        // Wait until consumer1 has actually read the message
        await consumer1Read.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);

        // Clear WITHOUT closing consumer1 — it is still actively polling
        broker.ClearGroup("live-group");
        broker.ClearTopic("live-consumer-clear-topic");

        using var producer2 = CreateProducer(broker.BootstrapServers);
        await producer2.ProduceAsync("live-consumer-clear-topic",
            new Message<string, string> { Key = "k2", Value = "after-clear" }, cts.Token);
        producer2.Flush(cts.Token);

        using var consumer2 = CreateConsumer(broker.BootstrapServers, "live-group", AutoOffsetReset.Earliest);
        consumer2.Subscribe("live-consumer-clear-topic");

        var received = new List<string>();
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) received.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        consumer1Cts.Cancel();
        await consumer1Task;
        consumer1.Close();
        consumer1.Dispose();

        received.Should().ContainSingle().Which.Should().Be("after-clear");
    }

    [Test]
    [Timeout(30000)]
    public async Task StaleCommitAfterClearGroup_PoisonsOffset_NewEarliestConsumerMissesMessage(CancellationToken cancellationToken)
    {
        // The bug: consumer1 reads a message but delays the Commit() call.
        // ClearGroup+ClearTopic runs and wipes state. Then consumer1.Commit() fires —
        // the stale committed offset (1) is written back into the now-clean broker.
        // consumer2 (Earliest, same group) sees committed=1, fetches from offset 1,
        // and misses "after-clear" which sits at offset 0.
        await using var broker = await BrokerFixture.CreateAndStartAsync("stale-commit-topic");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        using var producer1 = CreateProducer(broker.BootstrapServers);
        await producer1.ProduceAsync("stale-commit-topic",
            new Message<string, string> { Key = "k1", Value = "before-clear" }, cts.Token);
        producer1.Flush(cts.Token);

        using var consumer1 = CreateConsumer(broker.BootstrapServers, "stale-group", AutoOffsetReset.Earliest);
        consumer1.Subscribe("stale-commit-topic");

        ConsumeResult<string, string>? result = null;
        try
        {
            while (!cts.IsCancellationRequested && result == null)
            {
                var r = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (r?.Message?.Value != null) result = r;
            }
        }
        catch (OperationCanceledException) { }

        result.Should().NotBeNull();

        // Clear BEFORE consumer1 commits — simulates race with delayed auto-commit
        broker.ClearGroup("stale-group");
        broker.ClearTopic("stale-commit-topic");

        // Stale commit arrives AFTER the clear — broker rejects it with ILLEGAL_GENERATION
        try { consumer1.Commit(result!); } catch (KafkaException) { /* expected — broker rejected stale commit */ }
        consumer1.Close();

        // New message at offset 0 after clear
        using var producer2 = CreateProducer(broker.BootstrapServers);
        await producer2.ProduceAsync("stale-commit-topic",
            new Message<string, string> { Key = "k2", Value = "after-clear" }, cts.Token);
        producer2.Flush(cts.Token);

        // consumer2 Earliest: but committed offset is now 1 (from stale commit) → misses offset 0
        using var consumer2 = CreateConsumer(broker.BootstrapServers, "stale-group", AutoOffsetReset.Earliest);
        consumer2.Subscribe("stale-commit-topic");

        var received = new List<string>();
        using var shortCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            while (!shortCts.IsCancellationRequested)
            {
                var r = consumer2.Consume(TimeSpan.FromMilliseconds(300));
                if (r?.Message?.Value != null) received.Add(r.Message.Value);
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle().Which.Should().Be("after-clear",
            "stale commit must not poison the committed offset after ClearGroup");
    }

    // ── Multiple consumers / multiple producers tests ────────────────────────

    [Test]
    public async Task MultipleConsumers_SameGroup_ReceiveAllMessages_WithoutDuplicates()
    {
        // Two consumers in the same group share the partition (or both subscribe — in a
        // single-partition topic only one will own it, but they together must see all msgs).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await BrokerFixture.CreateAndStartAsync("mc-topic", 1, "mc-group");

        using var producer = CreateProducer(broker.BootstrapServers);
        var messages = new[] { "msg-1", "msg-2", "msg-3", "msg-4", "msg-5" };
        foreach (var m in messages)
            await producer.ProduceAsync("mc-topic", new Message<string, string> { Key = "k", Value = m }, cts.Token);
        producer.Flush(cts.Token);

        using var c1 = CreateConsumer(broker.BootstrapServers, "mc-group", AutoOffsetReset.Earliest);
        using var c2 = CreateConsumer(broker.BootstrapServers, "mc-group", AutoOffsetReset.Earliest);
        c1.Subscribe("mc-topic");
        c2.Subscribe("mc-topic");

        var received = new ConcurrentBag<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!drainCts.IsCancellationRequested && received.Count < messages.Length)
        {
            var r1 = c1.Consume(TimeSpan.FromMilliseconds(200));
            if (r1?.Message?.Value != null) received.Add(r1.Message.Value);
            var r2 = c2.Consume(TimeSpan.FromMilliseconds(200));
            if (r2?.Message?.Value != null) received.Add(r2.Message.Value);
        }

        received.Should().BeEquivalentTo(messages, "all messages must be consumed exactly once across the group");
    }

    [Test]
    public async Task MultipleConsumers_DifferentGroups_EachReceiveAllMessages()
    {
        // Two independent groups — each must receive the full set of messages.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await BrokerFixture.CreateAndStartAsync(
            ("multi-group-topic", 1));

        using var producer = CreateProducer(broker.BootstrapServers);
        var messages = new[] { "a", "b", "c" };
        foreach (var m in messages)
            await producer.ProduceAsync("multi-group-topic", new Message<string, string> { Key = "k", Value = m }, cts.Token);
        producer.Flush(cts.Token);

        using var cA = CreateConsumer(broker.BootstrapServers, "group-A", AutoOffsetReset.Earliest);
        using var cB = CreateConsumer(broker.BootstrapServers, "group-B", AutoOffsetReset.Earliest);
        cA.Subscribe("multi-group-topic");
        cB.Subscribe("multi-group-topic");

        var receivedA = new List<string>();
        var receivedB = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!drainCts.IsCancellationRequested &&
               (receivedA.Count < messages.Length || receivedB.Count < messages.Length))
        {
            if (receivedA.Count < messages.Length)
            {
                var r = cA.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) receivedA.Add(r.Message.Value);
            }
            if (receivedB.Count < messages.Length)
            {
                var r = cB.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) receivedB.Add(r.Message.Value);
            }
        }

        receivedA.Should().BeEquivalentTo(messages, "group-A must see all messages independently");
        receivedB.Should().BeEquivalentTo(messages, "group-B must see all messages independently");
    }

    [Test]
    public async Task MultipleProducers_SameGroup_ConsumerReceivesAll()
    {
        // Multiple producers write to the same topic — a single consumer must see all of them.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await BrokerFixture.CreateAndStartAsync("mp-topic", 1, "mp-group");

        using var p1 = CreateProducer(broker.BootstrapServers);
        using var p2 = CreateProducer(broker.BootstrapServers);

        await p1.ProduceAsync("mp-topic", new Message<string, string> { Key = "k", Value = "from-p1-1" }, cts.Token);
        await p2.ProduceAsync("mp-topic", new Message<string, string> { Key = "k", Value = "from-p2-1" }, cts.Token);
        await p1.ProduceAsync("mp-topic", new Message<string, string> { Key = "k", Value = "from-p1-2" }, cts.Token);
        await p2.ProduceAsync("mp-topic", new Message<string, string> { Key = "k", Value = "from-p2-2" }, cts.Token);
        p1.Flush(cts.Token);
        p2.Flush(cts.Token);

        using var consumer = CreateConsumer(broker.BootstrapServers, "mp-group", AutoOffsetReset.Earliest);
        consumer.Subscribe("mp-topic");

        var received = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!drainCts.IsCancellationRequested && received.Count < 4)
        {
            var r = consumer.Consume(TimeSpan.FromMilliseconds(300));
            if (r?.Message?.Value != null) received.Add(r.Message.Value);
        }

        received.Should().HaveCount(4);
        received.Should().Contain("from-p1-1").And.Contain("from-p1-2")
                .And.Contain("from-p2-1").And.Contain("from-p2-2");
    }

    [Test]
    public async Task MultipleProducers_TwoGroups_BothGroupsReceiveAllProducerMessages()
    {
        // Two producers, two independent consumer groups — each group sees all produced messages.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await BrokerFixture.CreateAndStartAsync("mp2g-topic", 1, "gX");

        using var p1 = CreateProducer(broker.BootstrapServers);
        using var p2 = CreateProducer(broker.BootstrapServers);

        await p1.ProduceAsync("mp2g-topic", new Message<string, string> { Key = "k", Value = "p1-msg" }, cts.Token);
        await p2.ProduceAsync("mp2g-topic", new Message<string, string> { Key = "k", Value = "p2-msg" }, cts.Token);
        p1.Flush(cts.Token);
        p2.Flush(cts.Token);

        using var cX = CreateConsumer(broker.BootstrapServers, "gX", AutoOffsetReset.Earliest);
        using var cY = CreateConsumer(broker.BootstrapServers, "gY", AutoOffsetReset.Earliest);
        cX.Subscribe("mp2g-topic");
        cY.Subscribe("mp2g-topic");

        var rxX = new List<string>();
        var rxY = new List<string>();
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!drainCts.IsCancellationRequested && (rxX.Count < 2 || rxY.Count < 2))
        {
            if (rxX.Count < 2) { var r = cX.Consume(TimeSpan.FromMilliseconds(200)); if (r?.Message?.Value != null) rxX.Add(r.Message.Value); }
            if (rxY.Count < 2) { var r = cY.Consume(TimeSpan.FromMilliseconds(200)); if (r?.Message?.Value != null) rxY.Add(r.Message.Value); }
        }

        rxX.Should().BeEquivalentTo(new[] { "p1-msg", "p2-msg" }, "gX must see both producers' messages");
        rxY.Should().BeEquivalentTo(new[] { "p1-msg", "p2-msg" }, "gY must see both producers' messages");
    }

    [Test]
    public async Task MultipleConsumers_SameGroup_AfterClearAndReproduce_AllReceiveNewMessages()
    {
        // After ClearGroup+ClearTopic, two consumers in the same group should together
        // read the newly produced messages from offset 0 (Earliest).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await BrokerFixture.CreateAndStartAsync("mc-clear-topic", 1, "mc-clear-group");

        using var p1 = CreateProducer(broker.BootstrapServers);
        await p1.ProduceAsync("mc-clear-topic", new Message<string, string> { Key = "k", Value = "old-msg" }, cts.Token);
        p1.Flush(cts.Token);

        // Consume and commit with consumer in the group
        using var oldConsumer = CreateConsumer(broker.BootstrapServers, "mc-clear-group", AutoOffsetReset.Earliest);
        oldConsumer.Subscribe("mc-clear-topic");
        using (var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
        {
            while (!drainCts.IsCancellationRequested)
            {
                var r = oldConsumer.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) { oldConsumer.Commit(r); break; }
            }
        }
        oldConsumer.Close();

        broker.ClearGroup("mc-clear-group");
        broker.ClearTopic("mc-clear-topic");

        using var p2 = CreateProducer(broker.BootstrapServers);
        await p2.ProduceAsync("mc-clear-topic", new Message<string, string> { Key = "k", Value = "new-1" }, cts.Token);
        await p2.ProduceAsync("mc-clear-topic", new Message<string, string> { Key = "k", Value = "new-2" }, cts.Token);
        p2.Flush(cts.Token);

        using var c1 = CreateConsumer(broker.BootstrapServers, "mc-clear-group", AutoOffsetReset.Earliest);
        using var c2 = CreateConsumer(broker.BootstrapServers, "mc-clear-group", AutoOffsetReset.Earliest);
        c1.Subscribe("mc-clear-topic");
        c2.Subscribe("mc-clear-topic");

        var received = new ConcurrentBag<string>();
        using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!readCts.IsCancellationRequested && received.Count < 2)
        {
            var r1 = c1.Consume(TimeSpan.FromMilliseconds(200));
            if (r1?.Message?.Value != null) received.Add(r1.Message.Value);
            var r2 = c2.Consume(TimeSpan.FromMilliseconds(200));
            if (r2?.Message?.Value != null) received.Add(r2.Message.Value);
        }

        received.Should().BeEquivalentTo(new[] { "new-1", "new-2" },
            "after clear, Earliest consumers must start from offset 0 and receive all new messages");
    }

    [Test]
    public async Task MultipleProducers_DifferentGroups_IndependentOffsets_AfterClear()
    {
        // Two groups track offsets independently; clearing one group doesn't affect the other.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await BrokerFixture.CreateAndStartAsync("indep-topic", 1, "grp-1");

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("indep-topic", new Message<string, string> { Key = "k", Value = "msg-A" }, cts.Token);
        producer.Flush(cts.Token);

        // grp-1 reads and commits
        using var c1 = CreateConsumer(broker.BootstrapServers, "grp-1", AutoOffsetReset.Earliest);
        c1.Subscribe("indep-topic");
        using (var d = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
        {
            while (!d.IsCancellationRequested)
            {
                var r = c1.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) { c1.Commit(r); break; }
            }
        }
        c1.Close();

        // grp-2 has NOT consumed yet
        // Now clear only grp-1 — grp-2 should still start from Earliest (offset 0)
        broker.ClearGroup("grp-1");

        using var c2 = CreateConsumer(broker.BootstrapServers, "grp-2", AutoOffsetReset.Earliest);
        c2.Subscribe("indep-topic");
        string? receivedByC2 = null;
        using (var d = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
        {
            while (!d.IsCancellationRequested && receivedByC2 == null)
            {
                var r = c2.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) receivedByC2 = r.Message.Value;
            }
        }
        c2.Close();

        receivedByC2.Should().Be("msg-A", "clearing grp-1 must not affect grp-2's offset tracking");

        // grp-1 after its own clear should also see the message again (Earliest from offset 0)
        using var c1b = CreateConsumer(broker.BootstrapServers, "grp-1", AutoOffsetReset.Earliest);
        c1b.Subscribe("indep-topic");
        string? receivedByC1b = null;
        using (var d = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
        {
            while (!d.IsCancellationRequested && receivedByC1b == null)
            {
                var r = c1b.Consume(TimeSpan.FromMilliseconds(200));
                if (r?.Message?.Value != null) receivedByC1b = r.Message.Value;
            }
        }

        receivedByC1b.Should().Be("msg-A", "grp-1 after ClearGroup must re-read from Earliest");
    }

    // ── ClearGroup race condition tests ──────────────────────────────────────

    [Test]
    [Timeout(5000)] // without fix: consumer waits ~SocketTimeoutMs (10 s) before recovering → test times out
    public async Task ClearGroup_DuringJoinGroupSettleWindow_ConsumerReceivesMessageWithoutHanging(CancellationToken cancellationToken)
    {
        // SettleJoinGroup fires 300 ms after JoinGroup is registered on the broker.
        // ClearGroup called in that window removes the session without canceling the
        // pending TCS — SettleJoinGroup wakes up, finds no session, returns silently,
        // and the consumer blocks until SocketTimeoutMs (~10 s) causes a reconnect.
        // With fix: TCS.TrySetCanceled() fires immediately → consumer retries in ~400 ms.
        await using var broker = await BrokerFixture.CreateAndStartAsync("join-hang-topic");

        using var producer = CreateProducer(broker.BootstrapServers);
        await producer.ProduceAsync("join-hang-topic",
            new Message<string, string> { Key = "k", Value = "msg" }, cancellationToken);
        producer.Flush(cancellationToken);

        using var consumer = CreateConsumer(broker.BootstrapServers, "join-hang-group", AutoOffsetReset.Earliest);
        consumer.Subscribe("join-hang-topic");

        // Drive the protocol loop so JoinGroup is sent and registered on the broker.
        try { consumer.Consume(TimeSpan.FromMilliseconds(20)); } catch (ConsumeException) { }
        // Wait long enough for JoinGroup to arrive (<<300 ms settle window).
        await Task.Delay(80, cancellationToken);

        // Clear inside the settle window — orphans the pending TCS (bug trigger).
        broker.ClearGroup("join-hang-group");

        // With fix: consumer gets REBALANCE_IN_PROGRESS instantly, rejoins in ~400 ms, receives message.
        // Without fix: consumer waits ~SocketTimeoutMs (10 s) → test times out at 5 s.
        var received = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(4));
        try
        {
            while (!cts.IsCancellationRequested && received.Count < 1)
            {
                try
                {
                    var r = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (r?.Message?.Value != null) received.Add(r.Message.Value);
                }
                catch (ConsumeException) { /* REBALANCE_IN_PROGRESS surfaced — consumer is retrying */ }
            }
        }
        catch (OperationCanceledException) { }

        received.Should().ContainSingle().Which.Should().Be("msg",
            "consumer must recover quickly after ClearGroup, not hang for SocketTimeoutMs");
    }
}
