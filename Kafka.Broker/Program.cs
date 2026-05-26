using Kafka.TestFramework;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Information);
            })
            .ConfigureServices((context, services) =>
            {
                services.AddSingleton<TestBroker>(sp =>
                {
                    var settings = context.Configuration
                        .GetSection("BrokerSettings")
                        .Get<BrokerSettings>() ?? new BrokerSettings();
                    return new TestBroker(settings);
                });
                services.AddHostedService<BrokerHostedService>();
            })
            .Build();

        await host.RunAsync();
    }
}

public class BrokerHostedService : IHostedService
{
    private readonly TestBroker _broker;
    private readonly ILogger<BrokerHostedService> _logger;

    public BrokerHostedService(TestBroker broker, ILogger<BrokerHostedService> logger)
    {
        _broker = broker;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _broker.StartAsync(cancellationToken);
        _logger.LogInformation("Test broker started. Bootstrap servers: {BootstrapServers}", _broker.BootstrapServers);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Test broker stopping");
        return Task.CompletedTask;
    }
}
