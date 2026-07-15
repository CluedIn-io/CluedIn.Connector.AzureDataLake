using System;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.FabricOpenMirroring.Connector.SqlDataWriter;

internal class OpenMirroringCsvSqlDataWriter : CsvSqlDataWriter
{
    protected override string GetFieldName(IStorageConfiguration configuration, string fieldName)
    {
        if (fieldName.Equals(StorageConfigurationConstants.ChangeTypeKey, StringComparison.Ordinal))
        {
            return OpenMirroringConfigurationConstants.RowMarkerKey;
        }

        return base.GetFieldName(configuration, fieldName);
    }

    protected override object GetValue(string key, SqlDataReader reader, IStorageConfiguration configuration)
    {
        var valueFromBase = base.GetValue(key, reader, configuration);
        return ValueHelper.TransformValueForRowMarker(key, valueFromBase);
    }
}
