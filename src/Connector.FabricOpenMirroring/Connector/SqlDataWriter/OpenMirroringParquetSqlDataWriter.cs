using System;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;

using Microsoft.Data.SqlClient;

using Parquet.Schema;

namespace CluedIn.Connector.FabricOpenMirroring.Connector.SqlDataWriter;

internal class OpenMirroringParquetSqlDataWriter : ParquetSqlDataWriter
{
    protected override DataField GetParquetDataField(string fieldName, Type type, IDataLakeJobData configuration)
    {
        if (fieldName.Equals(DataLakeConstants.ChangeTypeKey, StringComparison.Ordinal))
        {
            return new DataField(OpenMirroringConstants.RowMarkerKey, typeof(string));
        }

        return base.GetParquetDataField(fieldName, type, configuration);
    }

    protected override object GetValue(string key, SqlDataReader reader, IDataLakeJobData configuration)
    {
        var valueFromBase = base.GetValue(key, reader, configuration);
        return ValueHelper.TransformValueForRowMarker(key, valueFromBase);
    }
}
