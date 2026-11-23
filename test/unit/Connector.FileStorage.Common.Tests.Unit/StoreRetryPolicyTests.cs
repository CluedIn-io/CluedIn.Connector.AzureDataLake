using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Unit;

public class StoreRetryPolicyTests
{
    [Fact]
    public void Constructor_WithANegativeRetryCountOrDelay_UsesZero()
    {
        var policy = new StoreRetryPolicy(-1, TimeSpan.FromSeconds(-5));

        Assert.Equal(0, policy.RetryCount);
        Assert.Equal(TimeSpan.Zero, policy.RetryDelay);
    }

    [Fact]
    public void Constructor_UsesTheRetryCountAndDelay()
    {
        var policy = new StoreRetryPolicy(7, TimeSpan.FromMilliseconds(250));

        Assert.Equal(7, policy.RetryCount);
        Assert.Equal(TimeSpan.FromMilliseconds(250), policy.RetryDelay);
    }

    [Fact]
    public async Task RunAsync_WhenTheOperationSucceeds_RunsItOnceWithoutDelay()
    {
        var delays = new List<TimeSpan>();
        var policy = CreatePolicy(retryCount: 3, delays);
        var attempts = 0;

        var result = await policy.RunAsync(() => Task.FromResult(++attempts), NullLogger.Instance);

        Assert.Equal(1, result);
        Assert.Equal(1, attempts);
        Assert.Empty(delays);
    }

    [Fact]
    public async Task RunAsync_WhenTheOperationFailsOnce_RetriesAfterTheDelay()
    {
        var delays = new List<TimeSpan>();
        var policy = CreatePolicy(retryCount: 3, delays);
        var attempts = 0;

        var result = await policy.RunAsync(
            () => ++attempts == 1 ? throw new InvalidOperationException("transient") : Task.FromResult(attempts),
            NullLogger.Instance);

        Assert.Equal(2, result);
        Assert.Equal(new[] { TimeSpan.FromSeconds(5) }, delays);
    }

    [Fact]
    public async Task RunAsync_WhenTheOperationKeepsFailing_RetriesTheCountAndThrowsTheLastException()
    {
        var delays = new List<TimeSpan>();
        var policy = CreatePolicy(retryCount: 3, delays);
        var attempts = 0;

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => policy.RunAsync<int>(() => throw new InvalidOperationException($"attempt {++attempts}"), NullLogger.Instance));

        // The first attempt and 3 retries, with a delay before each retry
        Assert.Equal(4, attempts);
        Assert.Equal("attempt 4", exception.Message);
        Assert.Equal(3, delays.Count);
    }

    [Fact]
    public async Task RunAsync_WithoutRetries_RunsTheOperationOnce()
    {
        var delays = new List<TimeSpan>();
        var policy = CreatePolicy(retryCount: 0, delays);
        var attempts = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => policy.RunAsync<int>(() => throw new InvalidOperationException($"attempt {++attempts}"), NullLogger.Instance));

        Assert.Equal(1, attempts);
        Assert.Empty(delays);
    }

    [Fact]
    public async Task RunAsync_WhenAnAsyncOperationFails_RetriesIt()
    {
        var policy = CreatePolicy(retryCount: 2, new List<TimeSpan>());
        var attempts = 0;

        var result = await policy.RunAsync(
            async () =>
            {
                await Task.Yield();
                return ++attempts < 3 ? throw new InvalidOperationException() : attempts;
            },
            NullLogger.Instance);

        Assert.Equal(3, result);
    }

    [Theory]
    [InlineData(typeof(InvalidOperationException))]
    [InlineData(typeof(OperationCanceledException))]
    [InlineData(typeof(TimeoutException))]
    public async Task RunAsync_WhenTheFailureIsNotATransientSqlError_DoesNotRetry(Type exceptionType)
    {
        var delays = new List<TimeSpan>();
        var policy = new StoreRetryPolicy(3, TimeSpan.FromSeconds(5), delay =>
        {
            delays.Add(delay);
            return Task.CompletedTask;
        });
        var attempts = 0;

        await Assert.ThrowsAsync(
            exceptionType,
            () => policy.RunAsync<int>(
                () =>
                {
                    attempts++;
                    throw (Exception)Activator.CreateInstance(exceptionType);
                },
                NullLogger.Instance));

        Assert.Equal(1, attempts);
        Assert.Empty(delays);
    }

    [Fact]
    public async Task RunAsync_WhenTheFailureIsNotRetryable_ThrowsItAtOnce()
    {
        var delays = new List<TimeSpan>();
        var policy = new StoreRetryPolicy(
            3,
            TimeSpan.FromSeconds(5),
            delay =>
            {
                delays.Add(delay);
                return Task.CompletedTask;
            },
            exception => exception is TimeoutException);
        var attempts = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => policy.RunAsync<int>(() => throw new InvalidOperationException($"attempt {++attempts}"), NullLogger.Instance));
        Assert.Equal(1, attempts);

        attempts = 0;
        await Assert.ThrowsAsync<TimeoutException>(
            () => policy.RunAsync<int>(() => throw new TimeoutException($"attempt {++attempts}"), NullLogger.Instance));
        Assert.Equal(4, attempts);
        Assert.Equal(3, delays.Count);
    }

    [Theory]
    [InlineData(-2)] // timeout
    [InlineData(1205)] // deadlock victim
    [InlineData(1222)] // lock timeout
    [InlineData(-1)] // network-related error
    [InlineData(53)]
    [InlineData(64)]
    [InlineData(233)]
    [InlineData(10054)]
    [InlineData(10060)]
    [InlineData(40197)]
    [InlineData(40501)]
    [InlineData(40613)]
    [InlineData(49918)]
    public void IsTransientSqlErrorNumber_ForAnErrorThatCanGoAwayOnItsOwn_IsTrue(int number)
    {
        Assert.True(StoreRetryPolicy.IsTransientSqlErrorNumber(number));
    }

    [Theory]
    [InlineData(18456)] // login failed
    [InlineData(4060)] // cannot open the database
    [InlineData(229)] // permission denied
    [InlineData(208)] // invalid object name
    [InlineData(207)] // invalid column name
    [InlineData(102)] // incorrect syntax
    [InlineData(2627)] // primary key violation
    [InlineData(2601)] // duplicate key
    [InlineData(547)] // constraint violation
    [InlineData(245)] // conversion failed
    [InlineData(8152)] // string or binary data would be truncated
    [InlineData(2714)] // object already exists
    [InlineData(0)]
    public void IsTransientSqlErrorNumber_ForAnErrorThatRetryingCannotFix_IsFalse(int number)
    {
        Assert.False(StoreRetryPolicy.IsTransientSqlErrorNumber(number));
    }

    [Fact]
    public void IsRetryable_ForAnExceptionThatIsNotASqlError_IsFalse()
    {
        Assert.False(StoreRetryPolicy.IsRetryable(new InvalidOperationException("x", new TimeoutException())));
        Assert.False(StoreRetryPolicy.IsRetryable(new AggregateException(new InvalidOperationException(), new IndexOutOfRangeException())));
        Assert.False(StoreRetryPolicy.IsRetryable(null));
    }

    [Fact]
    public async Task RunAsync_WhenTheOperationFails_LogsEachRetry()
    {
        var logger = new CapturingLogger();
        var policy = CreatePolicy(retryCount: 2, new List<TimeSpan>());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => policy.RunAsync<int>(() => throw new InvalidOperationException(), logger));

        Assert.Equal(2, logger.Warnings.Count);
    }

    private static StoreRetryPolicy CreatePolicy(int retryCount, List<TimeSpan> delays)
    {
        return new StoreRetryPolicy(
            retryCount,
            TimeSpan.FromSeconds(5),
            delay =>
            {
                delays.Add(delay);
                return Task.CompletedTask;
            },
            _ => true);
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Warnings { get; } = new();

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
