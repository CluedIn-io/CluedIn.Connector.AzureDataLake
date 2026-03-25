using System;

namespace CluedIn.Connector.DataLake.Common;

internal class CacheTableHelper
{
    internal static string GetCacheTableName(Guid streamId)
    {
        return $"Stream_{streamId}";
    }

    public static string GetExportHistoryTableName(Guid streamId)
    {
        return GetCacheTableName(streamId) + "_ExportHistory";
    }
}
