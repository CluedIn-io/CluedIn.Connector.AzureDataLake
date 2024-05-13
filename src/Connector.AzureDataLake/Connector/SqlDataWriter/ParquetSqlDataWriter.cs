using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Parquet;
using Parquet.Schema;

using ParquetRow = Parquet.Rows.Row;
using ParquetTable = Parquet.Rows.Table;

namespace CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter;

internal class ParquetSqlDataWriter : SqlDataWriterBase
{
    // From documentation, it's ambiguous whether we need at least 5k or 50k rows to be optimal
    // TODO: Find recommendations or method to calculate row group threshold        
    public const int RowGroupThreshold = 10000;
    public override async Task<long> WriteOutputAsync(ExecutionContext context, Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
    {
        var fields = new List<Field>();
        foreach (var fieldName in fieldNames)
        {
            var type = reader.GetFieldType(fieldName);

            fields.Add(new DataField(fieldName, GetParquetDataType(type)));
        }

        var schema = new ParquetSchema(fields);
        var parquetTable = new ParquetTable(schema);
        using var parquetWriter = await ParquetWriter.CreateAsync(schema, outputStream);

        var totalProcessed = 0L;
        while (await reader.ReadAsync())
        {
            var fieldValues = fieldNames.Select(key => GetValue(key, reader));
            parquetTable.Add(new ParquetRow(fieldValues));

            totalProcessed++;

            if (totalProcessed % LoggingThreshold == 0)
            {
                context.Log.LogDebug("Written {Total} items.", totalProcessed);
            }

            if (totalProcessed % RowGroupThreshold == 0)
            {
                context.Log.LogDebug("Row group threshold {Threshold} reached. Current row {CurrentRow}. Writing data.", RowGroupThreshold, totalProcessed);
                await parquetWriter.WriteAsync(parquetTable);
                
                parquetTable = new ParquetTable(schema);
            }
        }

        if (totalProcessed % RowGroupThreshold != 0)
        {
            context.Log.LogDebug("Flushing remaining data. Current row {CurrentRow}", totalProcessed);
            await parquetWriter.WriteAsync(parquetTable);
        }

        return totalProcessed;
    }

    private Type GetParquetDataType(Type type)
    {
        if (type == typeof(string))
        {
            return type;
        }

        // TODO: Consider using DateTime and possibly additional column?
        // Parquet.Net does not support DateTimeOffset
        // Serialize it to string to prevent information loss
        var nullableUnderlyingType = Nullable.GetUnderlyingType(type);
        if (type == typeof(DateTimeOffset)
            || nullableUnderlyingType == typeof(DateTimeOffset))
        {
            return typeof(string);
        }

        var nullableType = typeof(Nullable<>);
        if (nullableUnderlyingType == null)
        {
            return nullableType.MakeGenericType(type);
        }

        return type;
    }

    protected override object GetValue(string key, SqlDataReader reader)
    {
        var value = base.GetValue(key, reader);
        if (value == null)
        {
            return null;
        }

        // TODO: Consider using DateTime and possibly additional column?
        // Parquet.Net does not support DateTimeOffset
        // Serialize it to string to prevent information loss

        if (value is DateTimeOffset offset)
        {
            return offset.ToString("o", CultureInfo.InvariantCulture);
        }

        var nullableUnderlyingType = Nullable.GetUnderlyingType(value.GetType());

        if (nullableUnderlyingType == typeof(DateTimeOffset))
        {
            return (value as DateTimeOffset?).Value.ToString("o", CultureInfo.InvariantCulture);
        }

        return value;
    }
}
