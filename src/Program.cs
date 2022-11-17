using DataProcessing.Data;
using DataProcessing.DataProcessing;
using DataProcessing.Other;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

var host = Host.CreateDefaultBuilder(args)
               .ConfigureServices(
                   services =>
                   {
                       services.AddLogging();
                       services.AddScoped<IDependency, Dependency>();
                       services.AddSingleton<IDataProcessor, BackgroundDataProcessor>();
                       services.AddHostedService(provider => (provider.GetRequiredService<IDataProcessor>() as BackgroundDataProcessor)!);

                       services.AddHostedService<Worker>();
                   }).ConfigureLogging(
                   log =>
                   {
                       log.AddFilter("Microsoft", level => level == LogLevel.Warning);
                       log.AddSimpleConsole(c =>
                       {
                           c.SingleLine = true;
                           c.ColorBehavior = LoggerColorBehavior.Enabled;
                           c.TimestampFormat = "[HH:mm:ss:ffff] ";
                       });  
                   })
               .Build();

await host.RunAsync();

public class Worker : BackgroundService
{
    private readonly IHost _host;

    private readonly IDataProcessor _dataProcessor;

    public Worker(IHost host, IDataProcessor dataProcessor)
    {
        _host = host;
        _dataProcessor = dataProcessor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _dataProcessor.ScheduleDataProcessing(new DataWithKey(1, "Data 1.1"));
        await _dataProcessor.ScheduleDataProcessing(new DataWithKey(2, "Data 2.1"));
        await _dataProcessor.ScheduleDataProcessing(new DataWithKey(1, "Data 1.2"));
        await _dataProcessor.ScheduleDataProcessing(new DataWithKey(3, "Data 3.1"));
        await _dataProcessor.ScheduleDataProcessing(new DataWithKey(1, "Data 1.3"));
        await Task.Delay(10000, stoppingToken);
        await _dataProcessor.ScheduleDataProcessing(new DataWithKey(2, "Data 2.2"));
        await Task.Delay(10000, stoppingToken);
        await _dataProcessor.ScheduleDataProcessing(new DataWithKey(3, "Data 3.2"));
        await Task.Delay(35000, stoppingToken);
        await _dataProcessor.ScheduleDataProcessing(new DataWithKey(3, "Data 3.3"));
        await Task.Delay(35000, stoppingToken);

        await _host.StopAsync(stoppingToken);
    }
}