using System.Threading.Channels;
using DataProcessing.Data;
using DataProcessing.Other;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DataProcessing.DataProcessing;

public class KeySpecificDataProcessor : IDataProcessor
{
    public int ProcessorKey { get; }

    public DateTime LastProcessingTimestamp => _processingFinishedTimestamp ?? DateTime.UtcNow;

    private DateTime? _processingFinishedTimestamp = DateTime.UtcNow;

    private bool Processing
    {
        set
        {
            if (!value)
            {
                _processingFinishedTimestamp = DateTime.UtcNow;
            }
            else
            {
                _processingFinishedTimestamp = null;
            }
        }
    }
    
    private Task? _processingTask;
    
    private readonly Channel<DataWithKey> _internalQueue = Channel.CreateUnbounded<DataWithKey>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

    private readonly IServiceScopeFactory _serviceScopeFactory;

    private readonly ILogger _logger;

    private KeySpecificDataProcessor(int processorKey, IServiceScopeFactory serviceScopeFactory, ILogger logger)
    {
        ProcessorKey = processorKey;
        _serviceScopeFactory = serviceScopeFactory;
        _logger = logger;
    }

    private void StartProcessing(CancellationToken cancellationToken = default)
    {
        _processingTask = Task.Factory.StartNew(
            async () =>
            {
                await foreach (var data in _internalQueue.Reader.ReadAllAsync(cancellationToken))
                {
                    Processing = true;
                    _logger.LogInformation("Received data: {Data}", data);
                    using (var dependenciesProvider = new DependenciesProvider(_serviceScopeFactory))
                    {
                        await ProcessData(data, dependenciesProvider.Dependency);
                    }
                    
                    Processing = _internalQueue.Reader.TryPeek(out _);
                }
            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }
    
    private async Task ProcessData(DataWithKey data, IDependency dependency)
    {
        await dependency.DoStuff();
    }
    
    public async Task ScheduleDataProcessing(DataWithKey dataWithKey)
    {
        if (dataWithKey.Key != ProcessorKey)
        {
            throw new InvalidOperationException($"Data with key {dataWithKey.Key} scheduled for KeySpecificDataProcessor with key {ProcessorKey}");
        }

        Processing = true;
        await _internalQueue.Writer.WriteAsync(dataWithKey);
    }

    public async Task StopProcessing()
    {
        _internalQueue.Writer.Complete();
        if (_processingTask != null)
        {
            await _processingTask;
        }
    }

    public static KeySpecificDataProcessor CreateAndStartProcessing(int processorKey, IServiceScopeFactory serviceScopeFactory, ILogger logger, CancellationToken processingCancellationToken = default)
    {
        var instance = new KeySpecificDataProcessor(processorKey, serviceScopeFactory, logger);
        instance.StartProcessing(processingCancellationToken);
        return instance;
    }

    private class DependenciesProvider : IDisposable
    {
        private readonly IServiceScope _scope;

        public IDependency Dependency { get; }

        public DependenciesProvider(IServiceScopeFactory serviceScopeFactory)
        {
            _scope = serviceScopeFactory.CreateScope();
            Dependency = _scope.ServiceProvider.GetRequiredService<IDependency>();
        }

        public void Dispose()
        {
            _scope.Dispose();
        }
    }
}