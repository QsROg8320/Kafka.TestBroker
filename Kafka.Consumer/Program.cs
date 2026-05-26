using ConsoleAppFramework;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Threading;

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
    /// Consumes messages from a Kafka topic and logs them to a file.
    /// </summary>
    /// <param name="topic">-t, Topic to subscribe to.</param>
    /// <param name="filePath">-f, File path to log messages.</param>
    [Command("consume")]
    public async Task Consume(
        [FromServices] IConfiguration configuration,
        [FromServices] ILogger<Commands> logger,
        string topic,
        string filePath,
        CancellationToken cancellationToken)
    {
        var consumerSettings = configuration.GetSection("ConsumerSettings").Get<ConsumerConfig>();
        if (consumerSettings == null)
        {
            logger.LogError("ConsumerSettings not found in appsettings.json");
            return;
        }

        if (string.IsNullOrEmpty(consumerSettings.GroupId))
        {
            logger.LogError("GroupId is not configured in appsettings.json");
            return;
        }

        consumerSettings.AutoOffsetReset = AutoOffsetReset.Earliest;

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerSettings).Build();
        consumer.Subscribe(topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(cancellationToken);
                var message = $"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.";
                logger.LogInformation("Consumed message '{Value}' at: '{TopicPartitionOffset}'",
                    consumeResult.Message.Value, consumeResult.TopicPartitionOffset);
                await File.AppendAllTextAsync(filePath, message + Environment.NewLine, cancellationToken);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            consumer.Close();
        }
    }
}
