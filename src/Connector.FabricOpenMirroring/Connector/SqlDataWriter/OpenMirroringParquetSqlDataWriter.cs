using System;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;

using Microsoft.Data.SqlClient;

using Parquet.Schema;

namespace CluedIn.Connector.FabricOpenMirroring.Connector.SqlDataWriter;

internal class OpenMirroringParquetSqlDataWriter : ParquetSqlDataWriter
{
    protected override DataField GetParquetDataField(string fieldName, Type type, IStorageConfiguration configuration)
    {
        if (fieldName.Equals(StorageConfigurationConstants.ChangeTypeKey, StringComparison.Ordinal))
        {
            return new DataField(OpenMirroringConfigurationConstants.RowMarkerKey, typeof(string));
        }

        return base.GetParquetDataField(fieldName, type, configuration);
    }

    protected override object GetValue(string key, SqlDataReader reader, IStorageConfiguration configuration)
    {
        var valueFromBase = base.GetValue(key, reader, configuration);
        return ValueHelper.TransformValueForRowMarker(key, valueFromBase);
    }
}
