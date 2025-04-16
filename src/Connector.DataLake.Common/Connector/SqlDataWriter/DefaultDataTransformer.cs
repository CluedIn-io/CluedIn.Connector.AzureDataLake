using System;
using System.Collections.Generic;

using Newtonsoft.Json;

namespace CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;

internal class DefaultDataTransformer : IDataTransformer
{
    public virtual IEnumerable<KeyValuePair<string, Type>> GetType(IDataLakeJobData configuration, KeyValuePair<string, Type> pair)
    {
        yield return pair;
        if (configuration.IsSerializedArrayColumnsEnabled && pair.Value == typeof(IEnumerable<string>))
        {
            yield return new KeyValuePair<string, Type>($"{pair.Key}_String", typeof(string));
        }
    }

    public virtual IEnumerable<KeyValuePair<string, object>> GetValue(IDataLakeJobData configuration, KeyValuePair<string, object> pair)
    {
        yield return pair;
        if (configuration.IsSerializedArrayColumnsEnabled && pair.Value is IEnumerable<string>)
        {
            yield return new KeyValuePair<string, object>($"{pair.Key}_String", JsonConvert.SerializeObject(pair.Value));
        }
    }
}
