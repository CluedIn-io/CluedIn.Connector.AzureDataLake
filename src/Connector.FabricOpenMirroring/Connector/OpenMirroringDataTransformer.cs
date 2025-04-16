using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;
using CluedIn.Core.Data.Parts;

using Newtonsoft.Json;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

internal class OpenMirroringDataTransformer : IDataTransformer
{
    private const string RowMarkerKey = "__rowMarker__";

    public IEnumerable<KeyValuePair<string, Type>> GetType(IDataLakeJobData configuration, KeyValuePair<string, Type> pair)
    {
        if (pair.Key == DataLakeConstants.ChangeTypeKey)
        {
            yield return new KeyValuePair<string, Type>(RowMarkerKey, typeof(string));
        }
        else if (pair.Value == typeof(IEnumerable<string>))
        {
            yield return new KeyValuePair<string, Type>(pair.Key, typeof(string));
        }
        else
        {
            yield return pair;
        }
    }

    public IEnumerable<KeyValuePair<string, object>> GetValue(IDataLakeJobData configuration, KeyValuePair<string, object> pair)
    {
        /*
            0 = INSERT
            1 = UPDATE
            2 = DELETE
            3 = UPSERT
         */
        if (pair.Key == DataLakeConstants.ChangeTypeKey)
        {
            var changeType = Enum.Parse<VersionChangeType>(pair.Value as string);
            var value = changeType switch
            {
                VersionChangeType.Removed => 2,
                _ => 3,
            };
            yield return new KeyValuePair<string, object>(RowMarkerKey, value.ToString());

        }
        else if (pair.Value is IEnumerable<string>)
        {
            yield return new KeyValuePair<string, object>(pair.Key, JsonConvert.SerializeObject(pair.Value));
        }
        else
        {
            yield return pair;
        }
    }
}
