using System;

namespace CluedIn.Connector.FileStorage.Common;

public interface ITimeProvider
{
    DateTimeOffset GetUtcNow();
}
