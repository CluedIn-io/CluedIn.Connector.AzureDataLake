using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core.Events;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Unit;

public class StreamCacheThrottleTests
{
    private static readonly TimeSpan WriteDelay = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Duration = TimeSpan.FromSeconds(30);
    private static readonly DateTimeOffset RequestedAt = new(2026, 11, 27, 12, 0, 0, TimeSpan.Zero);
    private readonly Guid _organizationId = Guid.NewGuid();
    private readonly Guid _streamId = Guid.NewGuid();

    [Fact]
    public void GetWriteDelay_NotRequested_IsZero()
    {
        // arrange
        var throttle = new StreamCacheThrottle(new SettableTimeProvider(RequestedAt), WriteDelay, Duration);

        // act
        var delay = throttle.GetWriteDelay(_organizationId, _streamId);

        // assert
        Assert.Equal(TimeSpan.Zero, delay);
    }

    [Fact]
    public void GetWriteDelay_WithinDurationOfRequest_IsWriteDelay()
    {
        // arrange
        var timeProvider = new SettableTimeProvider(RequestedAt);
        var throttle = new StreamCacheThrottle(timeProvider, WriteDelay, Duration);
        throttle.Request(CreateRequest(RequestedAt));

        // act
        timeProvider.Now = RequestedAt + Duration - TimeSpan.FromMilliseconds(1);
        var delay = throttle.GetWriteDelay(_organizationId, _streamId);

        // assert
        Assert.Equal(WriteDelay, delay);
    }

    [Fact]
    public void GetWriteDelay_DurationAfterRequest_IsZero()
    {
        // arrange
        var timeProvider = new SettableTimeProvider(RequestedAt);
        var throttle = new StreamCacheThrottle(timeProvider, WriteDelay, Duration);
        throttle.Request(CreateRequest(RequestedAt));

        // act
        timeProvider.Now = RequestedAt + Duration;
        var delay = throttle.GetWriteDelay(_organizationId, _streamId);

        // assert
        Assert.Equal(TimeSpan.Zero, delay);
    }

    [Fact]
    public void GetWriteDelay_OtherStreamOrOrganization_IsZero()
    {
        // arrange
        var throttle = new StreamCacheThrottle(new SettableTimeProvider(RequestedAt), WriteDelay, Duration);
        throttle.Request(CreateRequest(RequestedAt));

        // act
        var otherStreamDelay = throttle.GetWriteDelay(_organizationId, Guid.NewGuid());
        var otherOrganizationDelay = throttle.GetWriteDelay(Guid.NewGuid(), _streamId);

        // assert
        Assert.Equal(TimeSpan.Zero, otherStreamDelay);
        Assert.Equal(TimeSpan.Zero, otherOrganizationDelay);
    }

    [Fact]
    public void GetWriteDelay_NewerRequest_ExtendsAndOlderOneDoesNotShorten()
    {
        // arrange
        var timeProvider = new SettableTimeProvider(RequestedAt);
        var throttle = new StreamCacheThrottle(timeProvider, WriteDelay, Duration);
        throttle.Request(CreateRequest(RequestedAt));
        throttle.Request(CreateRequest(RequestedAt + TimeSpan.FromSeconds(25)));
        throttle.Request(CreateRequest(RequestedAt + TimeSpan.FromSeconds(5)));

        // act
        timeProvider.Now = RequestedAt + TimeSpan.FromSeconds(54);
        var delay = throttle.GetWriteDelay(_organizationId, _streamId);

        // assert
        Assert.Equal(WriteDelay, delay);
    }

    [Fact]
    public void GetWriteDelay_AfterExpiryAndNewRequest_IsWriteDelayAgain()
    {
        // arrange
        var timeProvider = new SettableTimeProvider(RequestedAt);
        var throttle = new StreamCacheThrottle(timeProvider, WriteDelay, Duration);
        throttle.Request(CreateRequest(RequestedAt));
        timeProvider.Now = RequestedAt + TimeSpan.FromMinutes(5);
        Assert.Equal(TimeSpan.Zero, throttle.GetWriteDelay(_organizationId, _streamId));

        // act
        throttle.Request(CreateRequest(timeProvider.Now));
        var delay = throttle.GetWriteDelay(_organizationId, _streamId);

        // assert
        Assert.Equal(WriteDelay, delay);
    }

    [Theory]
    [InlineData(0, 30_000)]
    [InlineData(10_000, 0)]
    public void GetWriteDelay_ZeroWriteDelayOrDuration_IsZero(int writeDelayMilliseconds, int durationMilliseconds)
    {
        // arrange
        var throttle = new StreamCacheThrottle(
            new SettableTimeProvider(RequestedAt),
            TimeSpan.FromMilliseconds(writeDelayMilliseconds),
            TimeSpan.FromMilliseconds(durationMilliseconds));
        throttle.Request(CreateRequest(RequestedAt));

        // act
        var delay = throttle.GetWriteDelay(_organizationId, _streamId);

        // assert
        Assert.Equal(TimeSpan.Zero, delay);
    }

    [Fact]
    public async Task Requester_UntilDisposed_PublishesRightAwayAndAtEveryInterval()
    {
        // arrange
        var published = new ConcurrentQueue<ThrottleStreamCacheRequestedEvent>();
        var systemEvents = CreateSystemEvents(published);
        var timeProvider = new SettableTimeProvider(RequestedAt);

        // act
        var requester = StreamCacheThrottleRequester.Start(
            systemEvents.Object, timeProvider, NullLogger.Instance, _organizationId, _streamId, TimeSpan.FromMilliseconds(50));
        await WaitUntilAsync(() => published.Count >= 3);
        await requester.DisposeAsync();
        var countWhenDisposed = published.Count;
        await Task.Delay(200);

        // assert
        Assert.Equal(countWhenDisposed, published.Count);
        Assert.All(published, request =>
        {
            Assert.Equal(_organizationId, request.OrganizationId);
            Assert.Equal(_streamId, request.StreamId);
            Assert.Equal(RequestedAt, request.RequestedAt);
        });
    }

    [Fact]
    public async Task Requester_PublishFails_KeepsPublishing()
    {
        // arrange
        var attempts = 0;
        var systemEvents = new Mock<ISystemEvents>();
        systemEvents
            .Setup(x => x.PublishAsync(It.IsAny<ThrottleStreamCacheRequestedEvent>()))
            .Returns(() =>
            {
                _ = Interlocked.Increment(ref attempts);
                return Task.FromException(new InvalidOperationException("bus is down"));
            });

        // act
        var requester = StreamCacheThrottleRequester.Start(
            systemEvents.Object, new SettableTimeProvider(RequestedAt), NullLogger.Instance, _organizationId, _streamId, TimeSpan.FromMilliseconds(20));
        await WaitUntilAsync(() => Volatile.Read(ref attempts) >= 2);
        await requester.DisposeAsync();

        // assert
        Assert.True(Volatile.Read(ref attempts) >= 2);
    }

    [Fact]
    public async Task Requester_ZeroInterval_PublishesNothing()
    {
        // arrange
        var published = new ConcurrentQueue<ThrottleStreamCacheRequestedEvent>();
        var systemEvents = CreateSystemEvents(published);

        // act
        var requester = StreamCacheThrottleRequester.Start(
            systemEvents.Object, new SettableTimeProvider(RequestedAt), NullLogger.Instance, _organizationId, _streamId, TimeSpan.Zero);
        await Task.Delay(100);
        await requester.DisposeAsync();
        await requester.DisposeAsync();

        // assert
        Assert.Empty(published);
    }

    private ThrottleStreamCacheRequestedEvent CreateRequest(DateTimeOffset requestedAt) => new()
    {
        OrganizationId = _organizationId,
        StreamId = _streamId,
        RequestedAt = requestedAt,
    };

    private static Mock<ISystemEvents> CreateSystemEvents(ConcurrentQueue<ThrottleStreamCacheRequestedEvent> published)
    {
        var systemEvents = new Mock<ISystemEvents>();
        systemEvents
            .Setup(x => x.PublishAsync(It.IsAny<ThrottleStreamCacheRequestedEvent>()))
            .Callback<ThrottleStreamCacheRequestedEvent>(published.Enqueue)
            .Returns(Task.CompletedTask);
        return systemEvents;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var giveUpAt = DateTime.UtcNow.AddSeconds(10);
        while (!condition() && DateTime.UtcNow < giveUpAt)
        {
            await Task.Delay(10);
        }

        Assert.True(condition());
    }

    private sealed class SettableTimeProvider : ITimeProvider
    {
        public SettableTimeProvider(DateTimeOffset now)
        {
            Now = now;
        }

        public DateTimeOffset Now { get; set; }

        public DateTimeOffset GetUtcNow() => Now;
    }
}
