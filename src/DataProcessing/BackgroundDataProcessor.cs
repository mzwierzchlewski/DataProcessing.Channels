using System.Threading.Channels;
using DataProcessing.Data;
using DataProcessing.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DataProcessing.DataProcessing;

public partial class BackgroundDataProcessor : BackgroundService, IDataProcessor
{
    private readonly Channel<DataWithKey> _internalQueue = Channel.CreateUnbounded<DataWithKey>(new UnboundedChannelOptions { SingleReader = true });

    private readonly Dictionary<int, KeySpecificDataProcessor> _dataProcessors = new();
    
    private readonly SemaphoreSlim _processorsLock = new(1, 1);

    private BackgroundDataProcessorMonitor? _monitor;

    private readonly IServiceScopeFactory _serviceScopeFactory;

    private readonly ILoggerFactory _loggerFactory;

    private readonly ILogger<BackgroundDataProcessor> _logger;

    public BackgroundDataProcessor(IServiceScopeFactory serviceScopeFactory, ILoggerFactory loggerFactory)
    {
        _serviceScopeFactory = serviceScopeFactory;
        _loggerFactory = loggerFactory;
        _logger = _loggerFactory.CreateLogger<BackgroundDataProcessor>();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _monitor = BackgroundDataProcessorMonitor.CreateAndStartMonitoring(_processorsLock, _dataProcessors, _loggerFactory.CreateLogger<BackgroundDataProcessorMonitor>(), stoppingToken);
        await foreach (var data in _internalQueue.Reader.ReadAllAsync(stoppingToken))
        {
            if (!await _processorsLock.WaitWithCancellation(stoppingToken))
            {
                break;
            }
            
            var processor = GetOrCreateDataProcessor(data, stoppingToken);
            await processor.ScheduleDataProcessing(data);

            _processorsLock.Release();
            _logger.LogInformation("Scheduled new data '{Data}' for processor with key '{Key}'", data, data.Key);
        }

        await _monitor.StopMonitoring();
    }
    
    private KeySpecificDataProcessor GetOrCreateDataProcessor(DataWithKey dataWithKey, CancellationToken newProcessorCancellationToken = default)
    {
        if (!_dataProcessors.TryGetValue(dataWithKey.Key, out var deviceProcessor))
        {
            var processor = CreateNewProcessor(dataWithKey.Key, newProcessorCancellationToken);
            _dataProcessors[dataWithKey.Key] = processor;
            deviceProcessor = processor;
            _logger.LogInformation("Created new processor for data key: {Key}", dataWithKey.Key);
        }

        return deviceProcessor;
    }

    private KeySpecificDataProcessor CreateNewProcessor(int dataKey, CancellationToken processorCancellationToken = default)
    {
        var logger = _loggerFactory.CreateLogger($"{typeof(KeySpecificDataProcessor).FullName}-{dataKey}");
        return KeySpecificDataProcessor.CreateAndStartProcessing(dataKey, _serviceScopeFactory, logger, processorCancellationToken);
    }

    public async Task ScheduleDataProcessing(DataWithKey dataWithKey) => await _internalQueue.Writer.WriteAsync(dataWithKey);
}