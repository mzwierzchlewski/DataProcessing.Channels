namespace DataProcessing.Infrastructure;

public static class SemaphoreSlimExtensions
{
    public static async Task<bool> WaitWithCancellation(this SemaphoreSlim semaphore, CancellationToken cancellationToken)
    {
        try
        {
            await semaphore.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return false;
        }

        return true;
    }
}