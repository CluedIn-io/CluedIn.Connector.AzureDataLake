using System;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.FabricOpenMirroring.Connector.SqlDataWriter;

internal class OpenMirroringCsvSqlDataWriter : CsvSqlDataWriter
{
    protected override string GetFieldName(IDataLakeJobData configuration, string fieldName)
    {
        if (fieldName.Equals(DataLakeConstants.ChangeTypeKey, StringComparison.Ordinal))
        {
            return OpenMirroringConstants.RowMarkerKey;
        }

        return base.GetFieldName(configuration, fieldName);
    }

    protected override object GetValue(string key, SqlDataReader reader, IDataLakeJobData configuration)
    {
        var valueFromBase = base.GetValue(key, reader, configuration);
        return ValueHelper.TransformValueForRowMarker(key, valueFromBase);
    }
}
