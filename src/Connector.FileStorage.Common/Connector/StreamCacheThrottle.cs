using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using CluedIn.Core.Events;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common.Connector;

/// <summary>
/// Sent while a stream is exported, to slow down the writes into the stream's cache table on every host, so that the
/// export's read of the table gets more of the database. There is no reply.
/// </summary>
public class ThrottleStreamCacheRequestedEvent : RemoteEvent
{
    public Guid OrganizationId { get; set; }

    public Guid StreamId { get; set; }

    /// <summary>
    /// When the export asked. The writes are delayed until this time plus the throttle duration.
    /// </summary>
    public DateTimeOffset RequestedAt { get; set; }
}

/// <summary>
/// The writers' side: which streams are throttled, and until when.
/// </summary>
internal sealed class StreamCacheThrottle
{
    private readonly ConcurrentDictionary<(Guid OrganizationId, Guid StreamId), DateTimeOffset> _throttledUntil = new();
    private readonly ITimeProvider _timeProvider;
    private readonly TimeSpan _writeDelay;
    private readonly TimeSpan _duration;

    public StreamCacheThrottle(ITimeProvider timeProvider, TimeSpan writeDelay, TimeSpan duration)
    {
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _writeDelay = writeDelay;
        _duration = duration;
    }

    public void Request(ThrottleStreamCacheRequestedEvent request)
    {
        if (_writeDelay <= TimeSpan.Zero || _duration <= TimeSpan.Zero)
        {
            return;
        }

        // A request that arrives late or out of order doesn't shorten the throttling of a newer one.
        var throttledUntil = request.RequestedAt + _duration;
        _ = _throttledUntil.AddOrUpdate(
            (request.OrganizationId, request.StreamId),
            throttledUntil,
            (_, current) => current > throttledUntil ? current : throttledUntil);
    }

    public TimeSpan GetWriteDelay(Guid organizationId, Guid streamId)
    {
        var key = (organizationId, streamId);
        if (!_throttledUntil.TryGetValue(key, out var throttledUntil))
        {
            return TimeSpan.Zero;
        }

        if (_timeProvider.GetUtcNow() < throttledUntil)
        {
            return _writeDelay;
        }

        // Only removes the entry if no newer request has extended it meanwhile.
        _ = _throttledUntil.TryRemove(new KeyValuePair<(Guid, Guid), DateTimeOffset>(key, throttledUntil));
        return TimeSpan.Zero;
    }

    public async Task DelayWriteAsync(Guid organizationId, Guid streamId)
    {
        var delay = GetWriteDelay(organizationId, streamId);
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay);
        }
    }
}

/// <summary>
/// The export's side: sends <see cref="ThrottleStreamCacheRequestedEvent"/> right away and then at every interval, until
/// it is disposed.
/// </summary>
internal sealed class StreamCacheThrottleRequester : IAsyncDisposable
{
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _requests;
    private bool _disposed;

    private StreamCacheThrottleRequester(Func<CancellationToken, Task> sendRequests)
    {
        // Not on the export's own flow, so that a slow event bus doesn't hold up the export.
        _requests = Task.Run(() => sendRequests(_stop.Token));
    }

    /// <param name="interval">Zero or less sends nothing.</param>
    public static StreamCacheThrottleRequester Start(
        ISystemEvents systemEvents,
        ITimeProvider timeProvider,
        ILogger logger,
        Guid organizationId,
        Guid streamId,
        TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
        {
            return new StreamCacheThrottleRequester(_ => Task.CompletedTask);
        }

        return new StreamCacheThrottleRequester(stop => SendRequestsUntilStoppedAsync(systemEvents, timeProvider, logger, organizationId, streamId, interval, stop));
    }

    private static async Task SendRequestsUntilStoppedAsync(
        ISystemEvents systemEvents,
        ITimeProvider timeProvider,
        ILogger logger,
        Guid organizationId,
        Guid streamId,
        TimeSpan interval,
        CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await systemEvents.PublishAsync(new ThrottleStreamCacheRequestedEvent
                {
                    OrganizationId = organizationId,
                    StreamId = streamId,
                    RequestedAt = timeProvider.GetUtcNow(),
                });
            }
            catch (Exception ex)
            {
                // The export doesn't depend on it, it only reads faster when the writers slow down.
                logger.LogWarning(ex, "Failed to ask the writers of stream {StreamId} to slow down during the export.", streamId);
            }

            try
            {
                await Task.Delay(interval, stop);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _stop.Cancel();
        await _requests;
        _stop.Dispose();
    }
}
