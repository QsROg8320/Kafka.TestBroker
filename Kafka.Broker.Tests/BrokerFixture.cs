using Kafka.TestFramework;

namespace Kafka.Broker.Tests;

internal static class BrokerFixture
{
    public static async Task<TestBroker> CreateAndStartAsync(
        string topicName = "test-topic",
        int partitions = 1,
        string groupId = "test-group")
    {
        return await CreateAndStartAsync(new[] { (topicName, partitions) });
    }

    public static async Task<TestBroker> CreateAndStartAsync(
        params (string Name, int Partitions)[] topics)
    {
        var settings = new BrokerSettings
        {
            Host = "127.0.0.1",
            Port = 0,
            Topics = topics.Select(t => new TopicSettings { Name = t.Name, Partitions = t.Partitions }).ToList(),
            Groups = new List<GroupSettings> { new GroupSettings { Id = "test-group" } },
        };

        var broker = new TestBroker(settings);
        await broker.StartAsync();
        return broker;
    }
}
