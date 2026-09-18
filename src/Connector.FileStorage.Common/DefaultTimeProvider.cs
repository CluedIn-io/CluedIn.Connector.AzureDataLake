using System;

namespace CluedIn.Connector.FileStorage.Common;

internal sealed class DefaultTimeProvider : ITimeProvider
{
    public DateTimeOffset GetUtcNow() => DateTimeOffset.UtcNow;
}
