using System;
using System.Collections.Generic;

namespace CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;

internal interface IDataTransformer
{
    IEnumerable<KeyValuePair<string, Type>> GetType(IDataLakeJobData configuration, KeyValuePair<string, Type> pair);
    IEnumerable<KeyValuePair<string, object>> GetValue(IDataLakeJobData configuration, KeyValuePair<string, object> pair);
}
