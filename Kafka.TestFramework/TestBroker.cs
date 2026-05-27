using Kafka.Protocol;
using Kafka.Protocol.Records;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static Kafka.Protocol.OffsetFetchResponse;
using Int16 = Kafka.Protocol.Int16;
using Int32 = Kafka.Protocol.Int32;
using Int64 = Kafka.Protocol.Int64;
using Record = Kafka.Protocol.Records.Record;
using String = Kafka.Protocol.String;

namespace Kafka.TestFramework
{
    // ── settings ─────────────────────────────────────────────────────────────────

    public class TopicSettings  { public string Name { get; set; } = "default-topic"; public int Partitions { get; set; } = 1; }
    public class GroupSettings  { public string Id   { get; set; } = "default-group"; }

    public class BrokerSettings
    {
        public string Host { get; set; } = "localhost";
        public int Port   { get; set; }
        public List<TopicSettings> Topics { get; set; } = new();
        public List<GroupSettings> Groups { get; set; } = new();
    }

    // ── partition log ─────────────────────────────────────────────────────────────
    //
    // Records are stored with OffsetDelta = absolute offset (stamped once at Append,
    // never mutated). BaseOffset=0 in FetchResponse means the consumer sees
    // consumer-offset = OffsetDelta directly.

    internal sealed class PartitionLog
    {
        private readonly List<Record> _records = new();
        private readonly object _lock = new();

        // Replaced atomically on every Append; completing it wakes ALL concurrent FetchRequests.
        private volatile TaskCompletionSource<bool> _dataReady =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public long HighWatermark { get; private set; }

        public long Append(IEnumerable<Record> incoming)
        {
            var list = incoming.ToList();
            if (list.Count == 0) return HighWatermark;

            long baseOffset;
            lock (_lock)
            {
                baseOffset = HighWatermark;
                var i = 0;
                foreach (var r in list)
                    r.OffsetDelta = Kafka.Protocol.VarInt.From((int)(baseOffset + i++));
                _records.AddRange(list);
                HighWatermark = _records.Count;
            }

            // Broadcast: swap in a fresh TCS so all awaiters unblock simultaneously.
            Interlocked.Exchange(ref _dataReady,
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously))
                .TrySetResult(true);

            return baseOffset;
        }

        public (List<Record> Records, long HighWatermark) ReadFrom(long fetchOffset)
        {
            lock (_lock)
            {
                var start = (int)Math.Max(0, fetchOffset);
                return start < _records.Count
                    ? (_records.GetRange(start, _records.Count - start), HighWatermark)
                    : (new List<Record>(), HighWatermark);
            }
        }

        // Completes when new records arrive or ct fires.
        public Task NewDataAsync(CancellationToken ct) => _dataReady.Task.WaitAsync(ct);

        public void Clear()
        {
            lock (_lock)
            {
                _records.Clear();
                HighWatermark = 0;
            }
        }
    }

    // ── group state ───────────────────────────────────────────────────────────────
    //
    // No leader, no rebalancing: each consumer is its own leader and gets all
    // partitions assigned immediately. GenerationId starts at 1 on first join and
    // is bumped by ClearGroup so the next Heartbeat kicks active consumers.

    internal sealed class GroupState
    {
        public int GenerationId;

        // Guards GenerationId + CommittedOffsets during ClearGroup / OffsetCommit.
        public readonly object Lock = new();

        // Shared across all consumers in the group; cleared by ClearGroup.
        public readonly ConcurrentDictionary<(string Topic, int Partition), long> CommittedOffsets = new();
    }

    // ── broker ────────────────────────────────────────────────────────────────────

    public sealed class TestBroker : IAsyncDisposable
    {
        private readonly SocketBasedKafkaTestFramework _server;
        private readonly BrokerSettings _settings;

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, PartitionLog>> _topics = new();
        private readonly ConcurrentDictionary<(string Topic, int Partition), short> _partitionErrors = new();
        private readonly ConcurrentDictionary<string, Uuid> _topicIds = new();
        private readonly ConcurrentDictionary<string, GroupState> _groups = new();

        private long _nextProducerId;
        private IAsyncDisposable? _running;

        public TestBroker(BrokerSettings settings)
        {
            _settings = settings;
            var addr = IPAddress.TryParse(settings.Host, out var ip)
                ? ip
                : Dns.GetHostAddresses(settings.Host)
                      .First(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
            _server = KafkaTestFramework.WithSocket(addr, settings.Port);
            RegisterHandlers();
        }

        public string BootstrapServers => $"{_settings.Host}:{_server.Port}";

        // ── public API ────────────────────────────────────────────────────────────

        public void SetPartitionError(string topic, int partition, short errorCode)
            => _partitionErrors[(topic, partition)] = errorCode;

        public void ClearPartitionError(string topic, int partition)
            => _partitionErrors.TryRemove((topic, partition), out _);

        public void ClearTopic(string topicName)
        {
            if (_topics.TryGetValue(topicName, out var parts))
                foreach (var log in parts.Values)
                    log.Clear();
        }

        public void ClearAllTopics()
        {
            foreach (var name in _topics.Keys)
                ClearTopic(name);
        }

        public void ClearGroup(string groupId)
        {
            var g = Group(groupId);
            lock (g.Lock)
            {
                g.GenerationId++;
                g.CommittedOffsets.Clear();
            }
        }

        public void ClearAllGroups()
        {
            foreach (var id in _groups.Keys)
                ClearGroup(id);
        }

        public Task StartAsync(CancellationToken ct = default)
        {
            _running = _server.Start();
            return Task.CompletedTask;
        }

        public async ValueTask DisposeAsync()
        {
            if (_running != null)
                await _running.DisposeAsync();
        }

        // ── helpers ───────────────────────────────────────────────────────────────

        private PartitionLog Log(string topic, int partition) =>
            _topics.GetOrAdd(topic, _ => new()).GetOrAdd(partition, _ => new PartitionLog());

        private GroupState Group(string groupId) =>
            _groups.GetOrAdd(groupId, _ => new GroupState());

        private Uuid TopicId(string name) =>
            _topicIds.GetOrAdd(name, n =>
            {
                using var ms  = new MemoryStream(Encoding.UTF8.GetBytes(n));
                using var sha = SHA256.Create();
                return Uuid.From(new Guid(sha.ComputeHash(ms).Take(16).ToArray()));
            });

        // ── FetchRequest partition read (called twice: before and after wait) ─────

        private record PartResult(
            string Topic, int Idx, long FetchOffset,
            List<Record> Records, long HighWatermark, short ErrorCode);

        private List<PartResult> ReadPartitions(FetchRequest request)
        {
            var results = new List<PartResult>();
            foreach (var t in request.TopicsCollection)
            {
                var topic = t.Topic;
                foreach (var p in t.PartitionsCollection)
                {
                    var idx    = p.Partition.Value;
                    var offset = p.FetchOffset.Value;

                    if (_partitionErrors.TryGetValue((topic, idx), out var err))
                    {
                        results.Add(new PartResult(topic, idx, offset, new List<Record>(), 0, err));
                        continue;
                    }

                    var (records, hwm) = Log(topic, idx).ReadFrom(offset);
                    if (records.Count > 0)
                        results.Add(new PartResult(topic, idx, offset, records, hwm, 0));
                }
            }
            return results;
        }

        private long GetCommittedOffset(string groupId, string topic, int partition)
        {
            var g = Group(groupId);
            return g.CommittedOffsets.TryGetValue((topic, partition), out var off) ? off : -1L;
        }

        // ── handlers ─────────────────────────────────────────────────────────────

        private void RegisterHandlers()
        {
            // ApiVersions ──────────────────────────────────────────────────────────
            _server.On<ApiVersionsRequest, ApiVersionsResponse>(request =>
                request.Respond()
                    .WithErrorCode(0)
                    .WithApiKeysCollection(
                        a => a.WithApiKey(18).WithMinVersion(0).WithMaxVersion(3),
                        a => a.WithApiKey( 3).WithMinVersion(0).WithMaxVersion(9),
                        a => a.WithApiKey(10).WithMinVersion(0).WithMaxVersion(3),
                        a => a.WithApiKey(11).WithMinVersion(0).WithMaxVersion(6),
                        a => a.WithApiKey( 2).WithMinVersion(0).WithMaxVersion(5),
                        a => a.WithApiKey(14).WithMinVersion(0).WithMaxVersion(4),
                        a => a.WithApiKey(12).WithMinVersion(0).WithMaxVersion(4),
                        a => a.WithApiKey( 9).WithMinVersion(0).WithMaxVersion(5),
                        a => a.WithApiKey( 1).WithMinVersion(0).WithMaxVersion(11),
                        a => a.WithApiKey( 0).WithMinVersion(0).WithMaxVersion(8),
                        a => a.WithApiKey( 8).WithMinVersion(0).WithMaxVersion(7),
                        a => a.WithApiKey( 4).WithMinVersion(0).WithMaxVersion(2),
                        a => a.WithApiKey(22).WithMinVersion(0).WithMaxVersion(4),
                        a => a.WithApiKey(45).WithMinVersion(0).WithMaxVersion(0)));

            // InitProducerId ───────────────────────────────────────────────────────
            _server.On<InitProducerIdRequest, InitProducerIdResponse>(request =>
                request.Respond()
                    .WithThrottleTimeMs(0)
                    .WithErrorCode(0)
                    .WithProducerId(Int64.From(Interlocked.Increment(ref _nextProducerId)))
                    .WithProducerEpoch(Int16.From(0)));

            // Metadata ─────────────────────────────────────────────────────────────
            _server.On<MetadataRequest, MetadataResponse>(request =>
            {
                var requested = request.TopicsCollection?
                    .Select(t => t.Name?.Value).Where(n => n != null).ToHashSet();

                var topics = (requested == null || requested.Count == 0)
                    ? _settings.Topics
                    : _settings.Topics.Where(t => requested.Contains(t.Name));

                var brokerIds = new[] { Int32.From(0) };

                return request.Respond()
                    .WithControllerId(Int32.From(0))
                    .WithClusterId(String.From("test-cluster"))
                    .WithBrokersCollection(b => b
                        .WithNodeId(Int32.From(0))
                        .WithHost(String.From(_settings.Host))
                        .WithPort(Int32.From(_server.Port))
                        .WithRack(String.From("test-rack")))
                    .WithTopicsCollection(topics.Select(t =>
                        new Func<MetadataResponse.MetadataResponseTopic, MetadataResponse.MetadataResponseTopic>(
                            rt => rt
                                .WithName(String.From(t.Name))
                                .WithTopicId(TopicId(t.Name))
                                .WithPartitionsCollection(
                                    Enumerable.Range(0, t.Partitions).Select(p =>
                                        new Func<MetadataResponse.MetadataResponseTopic.MetadataResponsePartition,
                                                 MetadataResponse.MetadataResponseTopic.MetadataResponsePartition>(
                                            rp => rp
                                                .WithPartitionIndex(Int32.From(p))
                                                .WithLeaderId(Int32.From(0))
                                                .WithReplicaNodesCollection(brokerIds)
                                                .WithIsrNodesCollection(brokerIds)))
                                    .ToArray())))
                    .ToArray());
            });

            // FindCoordinator ──────────────────────────────────────────────────────
            _server.On<FindCoordinatorRequest, FindCoordinatorResponse>(request =>
                request.Respond()
                    .WithErrorCode(0)
                    .WithNodeId(0)
                    .WithHost(_settings.Host)
                    .WithPort(_server.Port));

            // JoinGroup ────────────────────────────────────────────────────────────
            // No settle window, no leader election. Every consumer is its own leader
            // and receives only itself in the Members list. It then self-assigns all
            // partitions in SyncGroup without waiting for anyone else.
            _server.On<JoinGroupRequest, JoinGroupResponse>((request, _) =>
            {
                var groupId  = request.GroupId.Value ?? "";
                var memberId = string.IsNullOrEmpty(request.MemberId.Value)
                    ? Guid.NewGuid().ToString()
                    : request.MemberId.Value;

                var protocol = request.ProtocolsCollection.Any()
                    ? request.ProtocolsCollection.First().Value.Name.Value ?? "range"
                    : "range";
                var metadata = request.ProtocolsCollection.Any()
                    ? request.ProtocolsCollection.First().Value.Metadata
                    : Bytes.Default;

                var g = Group(groupId);
                // Initialise generation to 1 on first join; ClearGroup increments further.
                Interlocked.CompareExchange(ref g.GenerationId, 1, 0);

                return Task.FromResult(
                    request.Respond()
                        .WithErrorCode(0)
                        .WithGenerationId(g.GenerationId)
                        .WithMemberId(String.From(memberId))
                        .WithLeader(String.From(memberId))        // every consumer is its own leader
                        .WithProtocolName(String.From(protocol))
                        .WithMembersCollection(
                            new Func<JoinGroupResponse.JoinGroupResponseMember,
                                     JoinGroupResponse.JoinGroupResponseMember>(
                                mb => mb.WithMemberId(String.From(memberId)).WithMetadata(metadata))));
            });

            // SyncGroup ────────────────────────────────────────────────────────────
            // The consumer, being its own leader and seeing only itself in Members,
            // computes a full partition assignment for itself and sends it here.
            // We echo that assignment back immediately — no waiting.
            _server.On<SyncGroupRequest, SyncGroupResponse>((request, _) =>
            {
                if (request.GenerationId.Value != Group(request.GroupId.Value ?? "").GenerationId)
                    return Task.FromResult(
                        request.Respond().WithThrottleTimeMs(0).WithErrorCode(27).WithAssignment(Bytes.Default));

                var assignment = request.AssignmentsCollection.Any()
                    ? request.AssignmentsCollection.First().Assignment
                    : Bytes.Default;

                return Task.FromResult(
                    request.Respond().WithThrottleTimeMs(0).WithErrorCode(0).WithAssignment(assignment));
            });

            // Heartbeat ────────────────────────────────────────────────────────────
            // Returns REBALANCE_IN_PROGRESS when ClearGroup has bumped the generation.
            _server.On<HeartbeatRequest, HeartbeatResponse>(request =>
            {
                var g     = Group(request.GroupId.Value ?? "");
                var stale = g.GenerationId != request.GenerationId.Value;
                return request.Respond().WithErrorCode(stale ? (short)27 : (short)0);
            });

            // LeaveGroup ───────────────────────────────────────────────────────────
            _server.On<LeaveGroupRequest, LeaveGroupResponse>(request =>
                request.Respond().WithErrorCode(0));

            // OffsetFetch ──────────────────────────────────────────────────────────
            _server.On<OffsetFetchRequest, OffsetFetchResponse>(request =>
            {
                if (request.Version < 8)
                {
                    var gid = request.GroupId.Value ?? "";
                    var tps = ExpandTopicPartitions(
                        request.TopicsCollection?.Select(t =>
                            (t.Name.Value ?? "", t.PartitionIndexesCollection.Select(p => p.Value))));

                    return request.Respond().WithErrorCode(0).WithTopicsCollection(
                        tps.Select(tp =>
                            new Func<OffsetFetchResponse.OffsetFetchResponseTopic,
                                     OffsetFetchResponse.OffsetFetchResponseTopic>(rt =>
                                rt.WithName(tp.Topic)
                                  .WithPartitionsCollection(tp.Partitions.Select(p =>
                                      new Func<OffsetFetchResponse.OffsetFetchResponseTopic.OffsetFetchResponsePartition,
                                               OffsetFetchResponse.OffsetFetchResponseTopic.OffsetFetchResponsePartition>(rp =>
                                          rp.WithPartitionIndex(p)
                                            .WithCommittedOffset(Int64.From(GetCommittedOffset(gid, tp.Topic, p)))
                                            .WithMetadata(String.From(""))
                                            .WithErrorCode(0)))
                                  .ToArray()))
                        ).ToArray());
                }
                else
                {
                    return request.Respond().WithGroupsCollection(
                        request.GroupsCollection.Select(grp =>
                        {
                            var gid = grp.GroupId.Value ?? "";
                            var tps = ExpandTopicPartitions(
                                grp.TopicsCollection?.Select(t =>
                                    (t.Name.Value ?? "", t.PartitionIndexesCollection.Select(p => p.Value))));

                            return new Func<OffsetFetchResponseGroup, OffsetFetchResponseGroup>(rg =>
                                rg.WithGroupId(grp.GroupId)
                                  .WithTopicsCollection(tps.Select(tp =>
                                      new Func<OffsetFetchResponseGroup.OffsetFetchResponseTopics,
                                               OffsetFetchResponseGroup.OffsetFetchResponseTopics>(rt =>
                                          rt.WithName(tp.Topic)
                                            .WithPartitionsCollection(tp.Partitions.Select(p =>
                                                new Func<OffsetFetchResponseGroup.OffsetFetchResponseTopics.OffsetFetchResponsePartitions,
                                                         OffsetFetchResponseGroup.OffsetFetchResponseTopics.OffsetFetchResponsePartitions>(rp =>
                                                    rp.WithPartitionIndex(p)
                                                      .WithCommittedOffset(Int64.From(GetCommittedOffset(gid, tp.Topic, p)))
                                                      .WithMetadata(String.From(""))
                                                      .WithErrorCode(0)))
                                            .ToArray()))
                                  ).ToArray()));
                        }).ToArray());
                }
            });

            // ListOffsets ──────────────────────────────────────────────────────────
            _server.On<ListOffsetsRequest, ListOffsetsResponse>(request =>
                request.Respond().WithThrottleTimeMs(0).WithTopicsCollection(
                    request.TopicsCollection.Select(t =>
                        new Func<ListOffsetsResponse.ListOffsetsTopicResponse,
                                 ListOffsetsResponse.ListOffsetsTopicResponse>(rt =>
                            rt.WithName(t.Name)
                              .WithPartitionsCollection(t.PartitionsCollection.Select(p =>
                                  new Func<ListOffsetsResponse.ListOffsetsTopicResponse.ListOffsetsPartitionResponse,
                                           ListOffsetsResponse.ListOffsetsTopicResponse.ListOffsetsPartitionResponse>(rp =>
                                  {
                                      // Timestamp -2 = Earliest (offset 0); else Latest = HWM.
                                      var offset = p.Timestamp.Value == -2L
                                          ? 0L
                                          : Log(t.Name.Value!, p.PartitionIndex.Value).HighWatermark;
                                      return rp.WithPartitionIndex(p.PartitionIndex)
                                               .WithErrorCode(0)
                                               .WithOffset(Int64.From(offset))
                                               .WithTimestamp(Int64.From(-1))
                                               .WithLeaderEpoch(Int32.From(0));
                                  }))
                              .ToArray()))
                    ).ToArray()));

            // Fetch ────────────────────────────────────────────────────────────────
            _server.On<FetchRequest, FetchResponse>(async (request, ct) =>
            {
                var results = ReadPartitions(request);

                if (!results.Any(r => r.Records.Count > 0 || r.ErrorCode != 0) && request.MaxWaitMs.Value > 0)
                {
                    var signals = request.TopicsCollection
                        .SelectMany(t => t.PartitionsCollection.Select(p =>
                            Log(t.Topic, p.Partition.Value).NewDataAsync(ct)))
                        .ToList();

                    await Task.WhenAny(signals.Append(Task.Delay(request.MaxWaitMs.Value, ct)))
                        .ConfigureAwait(false);

                    results = ReadPartitions(request);
                }

                return request.Respond().WithResponsesCollection(
                    results.GroupBy(r => r.Topic).Select(grp =>
                        new Func<FetchResponse.FetchableTopicResponse, FetchResponse.FetchableTopicResponse>(ft =>
                            ft.WithTopicId(TopicId(grp.Key))
                              .WithTopic(grp.Key)
                              .WithPartitionsCollection(grp.Select(p =>
                                  new Func<FetchResponse.FetchableTopicResponse.PartitionData,
                                           FetchResponse.FetchableTopicResponse.PartitionData>(pd =>
                                  {
                                      pd = pd.WithPartitionIndex(p.Idx)
                                             .WithErrorCode(p.ErrorCode)
                                             .WithHighWatermark(p.HighWatermark);

                                      if (p.ErrorCode == 0 && p.Records.Count > 0)
                                          pd = pd.WithRecords(new NullableRecordBatch
                                          {
                                              BaseOffset      = 0,
                                              Magic           = 2,
                                              LastOffsetDelta = p.Records[^1].OffsetDelta.Value,
                                              Records         = new NullableArray<Record>(p.Records.ToArray())
                                          });

                                      return pd;
                                  }))
                              .ToArray()))
                    ).ToArray());
            });

            // Produce ──────────────────────────────────────────────────────────────
            _server.On<ProduceRequest, ProduceResponse>(request =>
                request.Respond().WithResponsesCollection(
                    request.TopicDataCollection.Select(td =>
                    {
                        var topicName = td.Value.Name.Value;
                        return new Func<ProduceResponse.TopicProduceResponse, ProduceResponse.TopicProduceResponse>(tr =>
                            tr.WithName(topicName)
                              .WithPartitionResponsesCollection(td.Value.PartitionDataCollection.Select(pd =>
                                  new Func<ProduceResponse.TopicProduceResponse.PartitionProduceResponse,
                                           ProduceResponse.TopicProduceResponse.PartitionProduceResponse>(pr =>
                                  {
                                      var idx = pd.Index.Value;

                                      if (_partitionErrors.TryGetValue((topicName, idx), out var err))
                                          return pr.WithIndex(idx).WithErrorCode(err)
                                                   .WithBaseOffset(-1).WithLogAppendTimeMs(-1);

                                      var baseOffset = Log(topicName, idx)
                                          .Append(pd.Records?.Records ?? Enumerable.Empty<Record>());

                                      return pr.WithIndex(idx).WithErrorCode(0)
                                               .WithBaseOffset(baseOffset).WithLogAppendTimeMs(-1);
                                  }))
                              .ToArray()));
                    }).ToArray()));

            // OffsetCommit ─────────────────────────────────────────────────────────
            _server.On<OffsetCommitRequest, OffsetCommitResponse>(request =>
            {
                var groupId = request.GroupId.Value ?? "";
                var reqGen  = request.GenerationIdOrMemberEpoch.Value;
                var g       = Group(groupId);

                bool valid;
                lock (g.Lock)
                {
                    valid = g.GenerationId == reqGen;
                    if (valid)
                        foreach (var t in request.TopicsCollection)
                        {
                            var topic = t.Name.Value ?? "";
                            foreach (var p in t.PartitionsCollection)
                                g.CommittedOffsets[(topic, p.PartitionIndex.Value)] = p.CommittedOffset.Value;
                        }
                }

                var code = valid ? (short)0 : (short)22; // 22 = ILLEGAL_GENERATION
                return request.Respond().WithTopicsCollection(
                    request.TopicsCollection.Select(t =>
                        new Func<OffsetCommitResponse.OffsetCommitResponseTopic,
                                 OffsetCommitResponse.OffsetCommitResponseTopic>(rt =>
                            rt.WithName(t.Name)
                              .WithPartitionsCollection(t.PartitionsCollection.Select(p =>
                                  new Func<OffsetCommitResponse.OffsetCommitResponseTopic.OffsetCommitResponsePartition,
                                           OffsetCommitResponse.OffsetCommitResponseTopic.OffsetCommitResponsePartition>(rp =>
                                      rp.WithPartitionIndex(p.PartitionIndex).WithErrorCode(code)))
                              .ToArray()))
                    ).ToArray());
            });

            // GetTelemetrySubscriptions ────────────────────────────────────────────
            _server.On<GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse>(request =>
                request.Respond().WithPushIntervalMs(1000));
        }

        // ── OffsetFetch helper ────────────────────────────────────────────────────

        private List<(string Topic, IReadOnlyList<int> Partitions)> ExpandTopicPartitions(
            IEnumerable<(string Topic, IEnumerable<int> Partitions)>? requested)
        {
            if (requested != null)
            {
                var list = requested.Select(t => (t.Topic, (IReadOnlyList<int>)t.Partitions.ToList())).ToList();
                if (list.Count > 0) return list;
            }

            return _settings.Topics
                .Select(t => (t.Name, (IReadOnlyList<int>)Enumerable.Range(0, t.Partitions).ToList()))
                .ToList();
        }
    }
}
