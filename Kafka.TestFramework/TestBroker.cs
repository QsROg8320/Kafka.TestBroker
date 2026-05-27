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
    public class TopicSettings
    {
        public string Name { get; set; } = "default-topic";
        public int Partitions { get; set; } = 1;
    }

    public class GroupSettings
    {
        public string Id { get; set; } = "default-group";
    }

    public class BrokerSettings
    {
        public string Host { get; set; } = "localhost";
        public int Port { get; set; }
        public List<TopicSettings> Topics { get; set; } = new();
        public List<GroupSettings> Groups { get; set; } = new();
    }

    public class TestBroker : IAsyncDisposable
    {
        private readonly SocketBasedKafkaTestFramework _testServer;
        private readonly BrokerSettings _settings;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, List<Record>>> _recordsByTopicAndPartition = new();
        private readonly ConcurrentDictionary<Uuid, string> _topicIdToNameMap = new();
        private readonly ConcurrentDictionary<(string Topic, int Partition), long> _partitionOffsets = new();
        private readonly ConcurrentDictionary<(string Group, string Topic, int Partition), long> _committedOffsets = new();
        private readonly ConcurrentDictionary<(string Topic, int Partition), short> _partitionErrors = new();
        private readonly ConcurrentDictionary<string, int> _groupLastGenerationId = new();
        private long _nextProducerId = 0;
        private IAsyncDisposable? _runningServer;

        private readonly object _joinLock = new();

        private class GroupSession
        {
            public int GenerationId { get; set; } = 0;
            public string LeaderId { get; set; } = "";
            public string ProtocolName { get; set; } = "";
            public Dictionary<string, (Bytes Metadata, TaskCompletionSource<JoinResult> Tcs)> PendingJoins { get; } = new();
            public bool SettleScheduled { get; set; } = false;

            public record JoinResult(
                int GenerationId,
                string LeaderId,
                string ProtocolName,
                List<(string MemberId, Bytes Metadata)> Members);
        }

        private readonly ConcurrentDictionary<string, GroupSession> _groupSessions = new();

        private class SyncSession
        {
            public TaskCompletionSource<Dictionary<string, Bytes>> AssignmentsTcs { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private readonly ConcurrentDictionary<(string GroupId, int GenerationId), SyncSession> _syncSessions = new();

        public TestBroker(BrokerSettings settings)
        {
            _settings = settings;
            var address = IPAddress.TryParse(_settings.Host, out var ip)
                ? ip
                : Dns.GetHostAddresses(_settings.Host).First(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
            _testServer = KafkaTestFramework.WithSocket(address, _settings.Port);
            SetupBroker();
        }

        public string BootstrapServers => $"{_settings.Host}:{_testServer.Port}";

        public void SetPartitionError(string topic, int partition, short errorCode)
            => _partitionErrors[(topic, partition)] = errorCode;

        public void ClearPartitionError(string topic, int partition)
            => _partitionErrors.TryRemove((topic, partition), out _);

        public void ClearTopic(string topicName)
        {
            if (_recordsByTopicAndPartition.TryGetValue(topicName, out var partitions))
            {
                foreach (var records in partitions.Values)
                    lock (records) records.Clear();
            }

            foreach (var key in _partitionOffsets.Keys.Where(k => k.Topic == topicName).ToList())
                _partitionOffsets.TryRemove(key, out _);

            foreach (var key in _committedOffsets.Keys.Where(k => k.Topic == topicName).ToList())
                _committedOffsets.TryRemove(key, out _);
        }

        public void ClearAllTopics()
        {
            foreach (var topicName in _recordsByTopicAndPartition.Keys.ToList())
                ClearTopic(topicName);
        }

        public void ClearGroup(string groupId)
        {
            lock (_joinLock)
            {
                if (_groupSessions.TryGetValue(groupId, out var clearedSession))
                {
                    _groupLastGenerationId[groupId] = clearedSession.GenerationId + 1;
                    foreach (var (_, tcs) in clearedSession.PendingJoins.Values)
                        tcs.TrySetCanceled();
                }
                _groupSessions.TryRemove(groupId, out _);
            }

            foreach (var key in _syncSessions.Keys.Where(k => k.GroupId == groupId).ToList())
            {
                if (_syncSessions.TryRemove(key, out var syncSession))
                    syncSession.AssignmentsTcs.TrySetCanceled();
            }

            foreach (var key in _committedOffsets.Keys.Where(k => k.Group == groupId).ToList())
                _committedOffsets.TryRemove(key, out _);
        }

        public void ClearAllGroups()
        {
            foreach (var groupId in _groupSessions.Keys.ToList())
                ClearGroup(groupId);
        }

        private void SetupBroker()
        {
            _testServer.On<ApiVersionsRequest, ApiVersionsResponse>(request =>
                request.Respond()
                    .WithErrorCode(0)
                    .WithApiKeysCollection(
                        api => api.WithApiKey(18).WithMinVersion(0).WithMaxVersion(3),
                        api => api.WithApiKey(3).WithMinVersion(0).WithMaxVersion(9),
                        api => api.WithApiKey(10).WithMinVersion(0).WithMaxVersion(3),
                        api => api.WithApiKey(11).WithMinVersion(0).WithMaxVersion(6),
                        api => api.WithApiKey(2).WithMinVersion(0).WithMaxVersion(5),
                        api => api.WithApiKey(14).WithMinVersion(0).WithMaxVersion(4),
                        api => api.WithApiKey(12).WithMinVersion(0).WithMaxVersion(4),
                        api => api.WithApiKey(9).WithMinVersion(0).WithMaxVersion(5),
                        api => api.WithApiKey(1).WithMinVersion(0).WithMaxVersion(11),
                        api => api.WithApiKey(0).WithMinVersion(0).WithMaxVersion(8),
                        api => api.WithApiKey(8).WithMinVersion(0).WithMaxVersion(7),
                        api => api.WithApiKey(4).WithMinVersion(0).WithMaxVersion(2),
                        api => api.WithApiKey(22).WithMinVersion(0).WithMaxVersion(4),
                        api => api.WithApiKey(45).WithMinVersion(0).WithMaxVersion(0)
                    ));

            _testServer.On<InitProducerIdRequest, InitProducerIdResponse>(request =>
            {
                var producerId = Interlocked.Increment(ref _nextProducerId);
                return request.Respond()
                    .WithThrottleTimeMs(0)
                    .WithErrorCode(0)
                    .WithProducerId(Int64.From(producerId))
                    .WithProducerEpoch(Int16.From(0));
            });

            _testServer.On<MetadataRequest, MetadataResponse>(request =>
            {
                var requestedTopicNames = request.TopicsCollection?
                    .Select(t => t.Name is { } n ? n.Value : null)
                    .Where(n => n != null)
                    .ToList();

                var topicsToDescribe = (requestedTopicNames == null || !requestedTopicNames.Any())
                    ? _settings.Topics
                    : _settings.Topics.Where(t => requestedTopicNames.Contains(t.Name));

                var topicConfigurators = topicsToDescribe.Select(topicSetting =>
                {
                    var topicId = Uuid.From(GetGuid(topicSetting.Name));
                    _topicIdToNameMap.AddOrUpdate(topicId, _ => topicSetting.Name, (_, _) => topicSetting.Name);
                    var brokerIds = new[] { Int32.From(0) };

                    return new Func<MetadataResponse.MetadataResponseTopic, MetadataResponse.MetadataResponseTopic>(
                        responseTopic => responseTopic
                            .WithName(String.From(topicSetting.Name))
                            .WithTopicId(topicId)
                            .WithPartitionsCollection(
                                Enumerable.Range(0, topicSetting.Partitions)
                                    .Select(p => new Func<MetadataResponse.MetadataResponseTopic.MetadataResponsePartition,
                                        MetadataResponse.MetadataResponseTopic.MetadataResponsePartition>(
                                        partition => partition
                                            .WithPartitionIndex(Int32.From(p))
                                            .WithLeaderId(Int32.From(0))
                                            .WithReplicaNodesCollection(brokerIds)
                                            .WithIsrNodesCollection(brokerIds)))
                                    .ToArray()));
                }).ToArray();

                return request.Respond()
                    .WithControllerId(Int32.From(0))
                    .WithClusterId(String.From("test-cluster"))
                    .WithBrokersCollection(broker => broker
                        .WithNodeId(Int32.From(0))
                        .WithHost(String.From(_settings.Host))
                        .WithPort(Int32.From(_testServer.Port))
                        .WithRack(String.From("test-rack")))
                    .WithTopicsCollection(topicConfigurators);
            });

            _testServer.On<FindCoordinatorRequest, FindCoordinatorResponse>(request =>
                request.Respond()
                    .WithErrorCode(0)
                    .WithNodeId(0)
                    .WithHost(_settings.Host)
                    .WithPort(_testServer.Port));

            _testServer.On<JoinGroupRequest, JoinGroupResponse>(async (request, cancellationToken) =>
            {
                var rawMemberId = request.MemberId.Value ?? "";
                var memberId = string.IsNullOrEmpty(rawMemberId) ? Guid.NewGuid().ToString() : rawMemberId;
                var groupId = request.GroupId.Value ?? "";
                var hasProtocol = request.ProtocolsCollection.Any();
                var protocolName = hasProtocol ? request.ProtocolsCollection.First().Value.Name.Value ?? "range" : "range";
                var metadata = hasProtocol ? request.ProtocolsCollection.First().Value.Metadata : Bytes.Default;

                TaskCompletionSource<GroupSession.JoinResult> tcs;

                lock (_joinLock)
                {
                    var session = _groupSessions.GetOrAdd(groupId, _ => new GroupSession
                    {
                        GenerationId = _groupLastGenerationId.TryGetValue(groupId, out var lastGen) ? lastGen : 0
                    });

                    tcs = new TaskCompletionSource<GroupSession.JoinResult>(
                        TaskCreationOptions.RunContinuationsAsynchronously);
                    session.PendingJoins[memberId] = (metadata, tcs);

                    if (string.IsNullOrEmpty(session.LeaderId))
                        session.LeaderId = memberId;

                    if (string.IsNullOrEmpty(session.ProtocolName))
                        session.ProtocolName = protocolName;

                    if (!session.SettleScheduled)
                    {
                        session.SettleScheduled = true;
                        _ = SettleJoinGroup(groupId, session);
                    }
                }

                GroupSession.JoinResult result;
                try
                {
                    result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
                }
                catch
                {
                    return request.Respond()
                        .WithErrorCode(27)
                        .WithGenerationId(-1)
                        .WithMemberId(String.From(memberId))
                        .WithLeader(String.From(""))
                        .WithProtocolName(String.From(""));
                }

                var isLeader = memberId == result.LeaderId;

                var membersForResponse = isLeader
                    ? result.Members
                    : result.Members.Where(m => m.MemberId == memberId).ToList();

                var memberFuncs = membersForResponse
                    .Select(m => new Func<JoinGroupResponse.JoinGroupResponseMember, JoinGroupResponse.JoinGroupResponseMember>(
                        member => member
                            .WithMemberId(String.From(m.MemberId))
                            .WithMetadata(m.Metadata)))
                    .ToArray();

                return request.Respond()
                    .WithErrorCode(0)
                    .WithGenerationId(result.GenerationId)
                    .WithMemberId(String.From(memberId))
                    .WithLeader(String.From(result.LeaderId))
                    .WithProtocolName(String.From(result.ProtocolName))
                    .WithMembersCollection(memberFuncs);
            });

            _testServer.On<SyncGroupRequest, SyncGroupResponse>(async (request, cancellationToken) =>
            {
                var groupId = request.GroupId.Value ?? "";
                var memberId = request.MemberId.Value ?? "";
                var generationId = request.GenerationId.Value;

                var syncSession = _syncSessions.GetOrAdd(
                    (groupId, generationId),
                    _ => new SyncSession());

                if (request.AssignmentsCollection.Any())
                {
                    var assignments = request.AssignmentsCollection
                        .ToDictionary(
                            a => a.MemberId.Value ?? "",
                            a => a.Assignment);
                    syncSession.AssignmentsTcs.TrySetResult(assignments);
                }

                Dictionary<string, Bytes> allAssignments;
                try
                {
                    allAssignments = await syncSession.AssignmentsTcs.Task
                        .WaitAsync(TimeSpan.FromSeconds(15), cancellationToken);
                }
                catch
                {
                    return request.Respond()
                        .WithThrottleTimeMs(0)
                        .WithErrorCode(27)
                        .WithAssignment(Bytes.Default);
                }

                var myAssignment = allAssignments.TryGetValue(memberId, out var a) ? a : Bytes.Default;

                return request.Respond()
                    .WithThrottleTimeMs(0)
                    .WithErrorCode(0)
                    .WithAssignment(myAssignment);
            });

            _testServer.On<OffsetFetchRequest, OffsetFetchResponse>(request =>
            {
                if (request.Version < 8)
                {
                    var groupIdStr = request.GroupId.Value ?? "";
                    var requestedTopics = request.TopicsCollection?.ToList()
                        ?? new List<OffsetFetchRequest.OffsetFetchRequestTopic>();

                    IEnumerable<(string TopicName, IEnumerable<int> Partitions)> topicPartitions;
                    if (requestedTopics.Any())
                    {
                        topicPartitions = requestedTopics.Select(t =>
                            (t.Name.Value ?? "",
                             (IEnumerable<int>)t.PartitionIndexesCollection.Select(p => p.Value)));
                    }
                    else
                    {
                        topicPartitions = _settings.Topics.Select(t =>
                            (t.Name, (IEnumerable<int>)Enumerable.Range(0, t.Partitions)));
                    }

                    var topicConfigurators = topicPartitions.Select(tp =>
                        new Func<OffsetFetchResponse.OffsetFetchResponseTopic, OffsetFetchResponse.OffsetFetchResponseTopic>(
                            responseTopic => responseTopic
                                .WithName(tp.TopicName)
                                .WithPartitionsCollection(
                                    tp.Partitions.Select(p =>
                                        new Func<OffsetFetchResponse.OffsetFetchResponseTopic.OffsetFetchResponsePartition,
                                            OffsetFetchResponse.OffsetFetchResponseTopic.OffsetFetchResponsePartition>(
                                            partition =>
                                            {
                                                var found = _committedOffsets.TryGetValue(
                                                    (groupIdStr, tp.TopicName, p), out var committed);
                                                return partition
                                                    .WithPartitionIndex(p)
                                                    .WithCommittedOffset(Int64.From(found ? committed : -1L))
                                                    .WithMetadata(String.From(""))
                                                    .WithErrorCode(0);
                                            }))
                                    .ToArray()))).ToArray();

                    return request.Respond()
                        .WithErrorCode(0)
                        .WithTopicsCollection(topicConfigurators);
                }
                else
                {
                    var groupsConfigurators = request.GroupsCollection.Select(groupInRequest =>
                    {
                        var groupId = groupInRequest.GroupId.Value ?? "";
                        var requestedTopics = groupInRequest.TopicsCollection?.ToList()
                            ?? new List<OffsetFetchRequest.OffsetFetchRequestGroup.OffsetFetchRequestTopics>();

                        IEnumerable<(string TopicName, IEnumerable<int> Partitions)> topicPartitions;
                        if (requestedTopics.Any())
                        {
                            topicPartitions = requestedTopics.Select(t =>
                                (t.Name.Value ?? "",
                                 (IEnumerable<int>)t.PartitionIndexesCollection.Select(p => p.Value)));
                        }
                        else
                        {
                            topicPartitions = _settings.Topics.Select(t =>
                                (t.Name, (IEnumerable<int>)Enumerable.Range(0, t.Partitions)));
                        }

                        return new Func<OffsetFetchResponseGroup, OffsetFetchResponseGroup>(
                            group => group
                                .WithGroupId(groupInRequest.GroupId)
                                .WithTopicsCollection(
                                    topicPartitions.Select(tp =>
                                        new Func<OffsetFetchResponseGroup.OffsetFetchResponseTopics,
                                            OffsetFetchResponseGroup.OffsetFetchResponseTopics>(
                                            responseTopic => responseTopic
                                                .WithName(tp.TopicName)
                                                .WithPartitionsCollection(
                                                    tp.Partitions.Select(p =>
                                                        new Func<
                                                            OffsetFetchResponseGroup.OffsetFetchResponseTopics.OffsetFetchResponsePartitions,
                                                            OffsetFetchResponseGroup.OffsetFetchResponseTopics.OffsetFetchResponsePartitions>(
                                                            partition =>
                                                            {
                                                                var found = _committedOffsets.TryGetValue(
                                                                    (groupId, tp.TopicName, p), out var committed);
                                                                return partition
                                                                    .WithPartitionIndex(p)
                                                                    .WithCommittedOffset(Int64.From(found ? committed : -1L))
                                                                    .WithMetadata(String.From(""))
                                                                    .WithErrorCode(0);
                                                            }))
                                                    .ToArray())))
                                    .ToArray()));
                    }).ToArray();

                    return request.Respond().WithGroupsCollection(groupsConfigurators);
                }
            });

            _testServer.On<ListOffsetsRequest, ListOffsetsResponse>(request =>
            {
                var topicFuncs = request.TopicsCollection.Select(topic =>
                {
                    var partitionFuncs = topic.PartitionsCollection.Select(partition =>
                    {
                        long offset = partition.Timestamp.Value == -2L
                            ? 0
                            : _partitionOffsets.TryGetValue(
                                (topic.Name.Value!, partition.PartitionIndex.Value),
                                out var hwm) ? hwm : 0;

                        return new Func<ListOffsetsResponse.ListOffsetsTopicResponse.ListOffsetsPartitionResponse,
                            ListOffsetsResponse.ListOffsetsTopicResponse.ListOffsetsPartitionResponse>(
                            p => p
                                .WithPartitionIndex(partition.PartitionIndex)
                                .WithErrorCode(0)
                                .WithOffset(Int64.From(offset))
                                .WithTimestamp(Int64.From(-1))
                                .WithLeaderEpoch(Int32.From(0)));
                    }).ToArray();

                    return new Func<ListOffsetsResponse.ListOffsetsTopicResponse, ListOffsetsResponse.ListOffsetsTopicResponse>(
                        t => t.WithName(topic.Name).WithPartitionsCollection(partitionFuncs));
                }).ToArray();

                return request.Respond()
                    .WithThrottleTimeMs(0)
                    .WithTopicsCollection(topicFuncs);
            });

            _testServer.On<FetchRequest, FetchResponse>(async (request, cancellationToken) =>
            {
                var topicData = request.TopicsCollection
                    .Select(topicRequest =>
                    {
                        var topicName = topicRequest.Topic;
                        var partitions = topicRequest.PartitionsCollection
                            .Select(p =>
                            {
                                var idx = p.Partition.Value;
                                var offset = p.FetchOffset.Value;

                                if (_partitionErrors.TryGetValue((topicName, idx), out var errCode))
                                    return (Index: idx, Offset: offset, Records: new List<Record>(),
                                            HighWatermark: 0L, ErrorCode: errCode);

                                List<Record> records = new();
                                if (_recordsByTopicAndPartition.TryGetValue(topicName, out var topicPartitions) &&
                                    topicPartitions.TryGetValue(idx, out var stored))
                                {
                                    lock (stored) { records = stored.SkipWhile(r => r.OffsetDelta.Value < offset).ToList(); }
                                }
                                _partitionOffsets.TryGetValue((topicName, idx), out var hwm);
                                return (Index: idx, Offset: offset, Records: records,
                                        HighWatermark: hwm, ErrorCode: (short)0);
                            })
                            .Where(p => p.Records.Any() || p.ErrorCode != 0)
                            .ToList();
                        return (TopicName: topicName, Partitions: partitions);
                    })
                    .Where(t => t.Partitions.Any())
                    .ToList();

                if (!topicData.Any() && request.MaxWaitMs.Value > 0)
                {
                    await Task.Delay(request.MaxWaitMs.Value, cancellationToken).ConfigureAwait(false);
                    // Re-check after the long-poll wait — records may have arrived during the delay.
                    topicData = request.TopicsCollection
                        .Select(topicRequest =>
                        {
                            var topicName = topicRequest.Topic;
                            var partitions = topicRequest.PartitionsCollection
                                .Select(p =>
                                {
                                    var idx = p.Partition.Value;
                                    var offset = p.FetchOffset.Value;

                                    if (_partitionErrors.TryGetValue((topicName, idx), out var errCode))
                                        return (Index: idx, Offset: offset, Records: new List<Record>(),
                                                HighWatermark: 0L, ErrorCode: errCode);

                                    List<Record> records = new();
                                    if (_recordsByTopicAndPartition.TryGetValue(topicName, out var topicPartitions) &&
                                        topicPartitions.TryGetValue(idx, out var stored))
                                    {
                                        lock (stored) { records = stored.SkipWhile(r => r.OffsetDelta.Value < offset).ToList(); }
                                    }
                                    _partitionOffsets.TryGetValue((topicName, idx), out var hwm);
                                    return (Index: idx, Offset: offset, Records: records,
                                            HighWatermark: hwm, ErrorCode: (short)0);
                                })
                                .Where(p => p.Records.Any() || p.ErrorCode != 0)
                                .ToList();
                            return (TopicName: topicName, Partitions: partitions);
                        })
                        .Where(t => t.Partitions.Any())
                        .ToList();
                }

                var topicConfigurators = topicData.Select(t =>
                    new Func<FetchResponse.FetchableTopicResponse, FetchResponse.FetchableTopicResponse>(
                        fetchableTopicResponse =>
                        {
                            var partitionConfigurators = t.Partitions.Select(p =>
                                new Func<FetchResponse.FetchableTopicResponse.PartitionData,
                                    FetchResponse.FetchableTopicResponse.PartitionData>(partitionData =>
                                {
                                    var pd = partitionData
                                        .WithPartitionIndex(p.Index)
                                        .WithErrorCode(p.ErrorCode)
                                        .WithHighWatermark(p.HighWatermark);

                                    if (p.ErrorCode == 0 && p.Records.Any())
                                    {
                                        // Records were stamped with absolute OffsetDelta at store time.
                                        // BaseOffset=0 means consumer computes: 0 + OffsetDelta = absolute offset.
                                        // LastOffsetDelta = absolute offset of the last returned record,
                                        // so librdkafka advances its position to LastOffsetDelta+1.
                                        pd = pd.WithRecords(new NullableRecordBatch
                                        {
                                            BaseOffset = 0,
                                            Magic = 2,
                                            LastOffsetDelta = p.Records[p.Records.Count - 1].OffsetDelta.Value,
                                            Records = new NullableArray<Record>(p.Records.ToArray())
                                        });
                                    }

                                    return pd;
                                })).ToArray();

                            return fetchableTopicResponse
                                .WithTopicId(GetGuid(t.TopicName))
                                .WithTopic(t.TopicName)
                                .WithPartitionsCollection(partitionConfigurators);
                        })).ToArray();

                return request.Respond().WithResponsesCollection(topicConfigurators);
            });

            _testServer.On<ProduceRequest, ProduceResponse>(request =>
            {
                var topicConfigurators = request.TopicDataCollection.Select(topicData =>
                {
                    var topicName = topicData.Value.Name.Value;
                    var topicPartitions = _recordsByTopicAndPartition
                        .GetOrAdd(topicName, _ => new ConcurrentDictionary<int, List<Record>>());

                    return new Func<ProduceResponse.TopicProduceResponse, ProduceResponse.TopicProduceResponse>(
                        topicProduceResponse => topicProduceResponse
                            .WithName(topicName)
                            .WithPartitionResponsesCollection(
                                topicData.Value.PartitionDataCollection.Select(partitionData =>
                                    new Func<ProduceResponse.TopicProduceResponse.PartitionProduceResponse,
                                        ProduceResponse.TopicProduceResponse.PartitionProduceResponse>(
                                        partitionProduceResponse =>
                                        {
                                            var partitionIndex = partitionData.Index.Value;

                                            if (_partitionErrors.TryGetValue((topicName, partitionIndex), out var errCode))
                                                return partitionProduceResponse
                                                    .WithIndex(partitionIndex)
                                                    .WithErrorCode(errCode)
                                                    .WithBaseOffset(-1)
                                                    .WithLogAppendTimeMs(-1);

                                            var records = topicPartitions.GetOrAdd(partitionIndex, _ => new List<Record>());
                                            var recordsToAdd = (partitionData.Records?.Records ?? Enumerable.Empty<Record>()).ToList();
                                            long baseOffset = 0;

                                            if (recordsToAdd.Any())
                                            {
                                                lock (records)
                                                {
                                                    baseOffset = _partitionOffsets.GetOrAdd((topicName, partitionIndex), 0);
                                                    // Stamp each record with its absolute partition offset as OffsetDelta.
                                                    // FetchResponse always uses BaseOffset=0, so OffsetDelta == absolute offset.
                                                    // This is set once here under the lock and never mutated again,
                                                    // eliminating the race when two consumer groups fetch the same
                                                    // partition at different offsets concurrently.
                                                    var idx = 0;
                                                    foreach (var r in recordsToAdd)
                                                        r.OffsetDelta = Kafka.Protocol.VarInt.From((int)(baseOffset + idx++));
                                                    records.AddRange(recordsToAdd);
                                                    _partitionOffsets[(topicName, partitionIndex)] = baseOffset + recordsToAdd.Count();
                                                }
                                            }

                                            return partitionProduceResponse
                                                .WithIndex(partitionIndex)
                                                .WithErrorCode(0)
                                                .WithBaseOffset(baseOffset)
                                                .WithLogAppendTimeMs(-1);
                                        }))
                                .ToArray()));
                }).ToArray();

                return request.Respond().WithResponsesCollection(topicConfigurators);
            });

            _testServer.On<OffsetCommitRequest, OffsetCommitResponse>(request =>
            {
                var groupId = request.GroupId.Value ?? "";
                var requestGenerationId = request.GenerationIdOrMemberEpoch.Value;

                // Reject stale commits: the generation in the request must match the current
                // session's generation. After ClearGroup the session is removed (or restarted
                // at a higher generation), so commits from a consumer that joined before the
                // clear carry a stale generationId and must not poison the committed offsets.
                var isValidCommit = _groupSessions.TryGetValue(groupId, out var currentSession)
                    && currentSession.GenerationId == requestGenerationId;

                if (isValidCommit)
                {
                    foreach (var topic in request.TopicsCollection)
                    {
                        var topicName = topic.Name.Value ?? "";
                        foreach (var partition in topic.PartitionsCollection)
                            _committedOffsets[(groupId, topicName, partition.PartitionIndex.Value)] =
                                partition.CommittedOffset.Value;
                    }
                }

                var errorCode = isValidCommit ? (short)0 : (short)22; // 22 = ILLEGAL_GENERATION
                return request.Respond()
                    .WithTopicsCollection(request.TopicsCollection.Select(topic =>
                        new Func<OffsetCommitResponse.OffsetCommitResponseTopic, OffsetCommitResponse.OffsetCommitResponseTopic>(
                            responseTopic => responseTopic
                                .WithName(topic.Name)
                                .WithPartitionsCollection(topic.PartitionsCollection.Select(partition =>
                                    new Func<OffsetCommitResponse.OffsetCommitResponseTopic.OffsetCommitResponsePartition,
                                        OffsetCommitResponse.OffsetCommitResponseTopic.OffsetCommitResponsePartition>(
                                        responsePartition => responsePartition
                                            .WithPartitionIndex(partition.PartitionIndex)
                                            .WithErrorCode(errorCode)))
                                .ToArray()))
                    ).ToArray());
            });

            _testServer.On<LeaveGroupRequest, LeaveGroupResponse>(request =>
            {
                var groupId = request.GroupId.Value ?? "";
                if (_groupSessions.TryGetValue(groupId, out var session))
                {
                    lock (_joinLock)
                    {
                        var leavingId = request.MemberId.Value ?? "";
                        if (!string.IsNullOrEmpty(leavingId))
                        {
                            session.PendingJoins.Remove(leavingId);
                            if (session.LeaderId == leavingId)
                                session.LeaderId = "";
                        }
                    }
                }
                return request.Respond().WithErrorCode(0);
            });

            _testServer.On<HeartbeatRequest, HeartbeatResponse>(request =>
            {
                var groupId = request.GroupId.Value ?? "";
                var generationId = request.GenerationId.Value;

                // If the session was cleared or generation doesn't match, signal rebalance.
                // This kicks consumers that are stuck in FetchRequest and missed the ClearGroup.
                var isStale = !_groupSessions.TryGetValue(groupId, out var session)
                    || session.GenerationId != generationId;

                return request.Respond().WithErrorCode(isStale ? (short)27 : (short)0);
            });

            _testServer.On<GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse>(request =>
                request.Respond().WithPushIntervalMs(1000));
        }

        private async Task SettleJoinGroup(string groupId, GroupSession expectedSession)
        {
            await Task.Delay(300).ConfigureAwait(false);

            List<(string MemberId, Bytes Metadata, TaskCompletionSource<GroupSession.JoinResult> Tcs)> toSettle;
            int generationId;
            string leaderId, protocolName;

            lock (_joinLock)
            {
                // Guard against stale settle: if ClearGroup replaced the session with a new object,
                // bail out. The new session's own SettleJoinGroup will handle it correctly.
                if (!_groupSessions.TryGetValue(groupId, out var session) || !ReferenceEquals(session, expectedSession))
                    return;

                session.GenerationId++;
                generationId = session.GenerationId;
                leaderId = session.LeaderId;
                protocolName = session.ProtocolName;

                toSettle = session.PendingJoins
                    .Select(kvp => (kvp.Key, kvp.Value.Metadata, kvp.Value.Tcs))
                    .ToList();

                // If the current leader didn't rejoin (e.g. crashed without LeaveGroup),
                // elect the first rejoining member so SyncGroup can complete.
                if (toSettle.Any() && !toSettle.Any(m => m.MemberId == leaderId))
                {
                    leaderId = toSettle[0].MemberId;
                    session.LeaderId = leaderId;
                }

                session.PendingJoins.Clear();
                session.SettleScheduled = false;
            }

            var members = toSettle.Select(m => (m.MemberId, m.Metadata)).ToList();
            var result = new GroupSession.JoinResult(generationId, leaderId, protocolName, members);

            foreach (var (_, _, tcs) in toSettle)
                tcs.TrySetResult(result);
        }

        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            _runningServer = _testServer.Start();
            return Task.CompletedTask;
        }

        public async ValueTask DisposeAsync()
        {
            if (_runningServer != null)
                await _runningServer.DisposeAsync();
        }

        private Guid GetGuid(string value)
        {
            using var input = new MemoryStream(Encoding.UTF8.GetBytes(value));
            using var sha256 = SHA256.Create();
            var hash = sha256.ComputeHash(input);
            return new Guid(hash.Take(16).ToArray());
        }
    }
}
