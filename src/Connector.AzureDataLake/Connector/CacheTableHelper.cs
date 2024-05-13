using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake.Connector;

internal class CacheTableHelper
{
    internal static string GetCacheTableName(Guid streamId)
    {
        return $"Stream_{streamId}";
    }
}
