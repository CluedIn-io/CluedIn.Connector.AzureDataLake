using System;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Core.Data.Parts;

namespace CluedIn.Connector.FabricOpenMirroring.Connector.SqlDataWriter;

internal class ValueHelper
{
    public static object TransformValueForRowMarker(string key, object valueFromBase)
    {
        /*
            0 = INSERT
            1 = UPDATE
            2 = DELETE
            3 = UPSERT
         */
        if (key.Equals(DataLakeConstants.ChangeTypeKey, StringComparison.Ordinal))
        {
            var changeType = Enum.Parse<VersionChangeType>(valueFromBase as string);
            var value = changeType switch
            {
                VersionChangeType.Removed => 2,
                _ => 4,
            };
            return value.ToString();
        }

        return valueFromBase;
    }
}
