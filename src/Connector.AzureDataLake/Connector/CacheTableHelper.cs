using System;

namespace CluedIn.Connector.AzureDataLake.Connector;

internal class CacheTableHelper
{
    internal static string GetCacheTableName(Guid streamId)
    {
        return $"Stream_{streamId}";
    }
}
