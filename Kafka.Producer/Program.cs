using ConsoleAppFramework;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.IO;

var app = ConsoleApp.Create()
    .ConfigureServices((context, services) =>
    {
        services.AddSingleton<IConfiguration>(context);
        services.AddLogging(b => b.AddConsole());
    })
    .ConfigureDefaultConfiguration();

app.Add<Commands>();
await app.RunAsync(args);

public class Commands
{
    /// <summary>
    /// Produces a message to a Kafka topic from a file.
    /// </summary>
    /// <param name="topic">-t, Topic to produce to.</param>
    /// <param name="key">-k, Message key.</param>
    /// <param name="filePath">-f, File path containing the message.</param>
    [Command("produce")]
    public async Task Produce(
        [FromServices] IConfiguration configuration,
        [FromServices] ILogger<Commands> logger,
        string topic,
        string key,
        string filePath)
    {
        var producerSettings = configuration.GetSection("ProducerSettings").Get<ProducerConfig>();
        if (producerSettings == null)
        {
            logger.LogError("ProducerSettings not found in appsettings.json");
            return;
        }
        var message = await File.ReadAllTextAsync(filePath);

        using var producer = new ProducerBuilder<string, string>(producerSettings).Build();
        try
        {
            var deliveryResult = await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = message });
            producer.Flush();
            logger.LogInformation("Delivered message to {TopicPartitionOffset}", deliveryResult.TopicPartitionOffset);
        }
        catch (ProduceException<string, string> e)
        {
            logger.LogError("Delivery failed: {Reason}", e.Error.Reason);
        }
    }
}
