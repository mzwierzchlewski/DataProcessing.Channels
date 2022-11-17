using Microsoft.Extensions.Logging;

namespace DataProcessing.Other;

public interface IDependency
{
    Task DoStuff();
}

public class Dependency : IDependency
{
    // Random identifier will help verify the scoped lifetime of the dependency
    private readonly string _dependencyId = Path.GetRandomFileName();

    private readonly ILogger<Dependency> _logger;

    public Dependency(ILogger<Dependency> logger)
    {
        _logger = logger;
    }

    public Task DoStuff()
    {
        _logger.LogInformation("Dependency {DependencyId} doing stuff", _dependencyId);
        return Task.CompletedTask;
    }
}