using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;

using Microsoft.Data.SqlClient;

using Parquet.Schema;

namespace CluedIn.Connector.FabricOpenMirroring.Connector.SqlDataWriter;

internal class OpenMirroringParquetSqlDataWriter : ParquetSqlDataWriter
{
    private const string RowMarkerKey = "__rowMarker__";

    protected override ICollection<string> OrderFields(ExecutionContext context, IDataLakeJobData configuration, ICollection<string> fieldNames)
    {
        var orderedFields = new List<string>(fieldNames);
        orderedFields.Remove(DataLakeConstants.ChangeTypeKey);
        orderedFields.Add(DataLakeConstants.ChangeTypeKey);

        return orderedFields;
    }
    protected override DataField GetParquetDataField(string fieldName, Type type, IDataLakeJobData configuration)
    {
        if (fieldName.Equals(DataLakeConstants.ChangeTypeKey, StringComparison.Ordinal))
        {
            return new DataField(RowMarkerKey, typeof(string));
        }

        return base.GetParquetDataField(fieldName, type, configuration);
    }

    protected override object GetValue(string key, SqlDataReader reader, IDataLakeJobData configuration)
    {
        /*
            0 = INSERT
            1 = UPDATE
            2 = DELETE
            3 = UPSERT
         */
        var valueFromBase = base.GetValue(key, reader, configuration);
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
