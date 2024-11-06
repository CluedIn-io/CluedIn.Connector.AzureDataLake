using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CluedIn.Core;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Parquet;
using Parquet.Schema;

using ParquetRow = Parquet.Rows.Row;
using ParquetTable = Parquet.Rows.Table;

namespace CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;

internal class ParquetSqlDataWriter : SqlDataWriterBase
{
    // From documentation, it's ambiguous whether we need at least 5k or 50k rows to be optimal
    // TODO: Find recommendations or method to calculate row group threshold        
    public const int RowGroupThreshold = 10000;
    private static readonly Regex NonAlphaNumericRegex = new ("[^a-zA-Z0-9_]");
    public override async Task<long> WriteOutputAsync(
        ExecutionContext context,
        IDataLakeJobData configuration,
        Stream outputStream,
        ICollection<string> fieldNames,
        SqlDataReader reader)
    {
        var fields = new List<Field>();
        foreach (var fieldName in fieldNames)
        {
            var type = reader.GetFieldType(fieldName);

            var parquetFieldName = configuration.ShouldEscapeVocabularyKeys ? EscapeVocabularyKey(fieldName) : fieldName;
            if (fieldName == "Codes" || fieldName == "OutgoingEdges" || fieldName == "IncomingEdges")
            {
                fields.Add(new DataField(parquetFieldName, typeof(IEnumerable<string>)));
            }
            else
            {
                fields.Add(new DataField(parquetFieldName, GetParquetDataType(type, configuration)));
            }
        }

        var schema = new ParquetSchema(fields);
        var parquetTable = new ParquetTable(schema);
        using var parquetWriter = await ParquetWriter.CreateAsync(schema, outputStream);

        var totalProcessed = 0L;

        while (await reader.ReadAsync())
        {
            var fieldValues = fieldNames.Select(key => GetValue(key, reader, configuration));
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

    private string EscapeVocabularyKey(string fieldName)
    {
        return NonAlphaNumericRegex.Replace(fieldName, "_");
    }

    private static Type GetParquetDataType(Type type, IDataLakeJobData configuration)
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

        if (configuration.ShouldWriteGuidAsString)
        {
            if (type == typeof(Guid)
                || nullableUnderlyingType == typeof(Guid))
            {
                return typeof(string);
            }
        }

        var nullableType = typeof(Nullable<>);
        if (nullableUnderlyingType == null)
        {
            return nullableType.MakeGenericType(type);
        }

        return type;
    }

    protected override object GetValue(string key, SqlDataReader reader, IDataLakeJobData configuration)
    {
        var value = base.GetValue(key, reader, configuration);
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

        if (configuration.ShouldWriteGuidAsString && value is Guid guid)
        {
            return guid.ToString();
        }

        var nullableUnderlyingType = Nullable.GetUnderlyingType(value.GetType());

        if (nullableUnderlyingType == typeof(DateTimeOffset))
        {
            return (value as DateTimeOffset?).Value.ToString("o", CultureInfo.InvariantCulture);
        }

        if (key == "Codes")
        {
            return JsonConvert.DeserializeObject<string[]>(value.ToString());
        }

        if (key == "OutgoingEdges" || key == "IncomingEdges")
        {
            return GetEdgesData(value);
        }

        return value;
    }

    private static string[] GetEdgesData(object value)
    {
        var edges = JsonConvert.DeserializeObject<List<JObject>>(value.ToString());

        var result = edges
            .Select(s => string.Format("EdgeType: {0}; From: {1}; To: {2}; Properties: {3}",
                s["EdgeType"]["Code"].ToString(),
                s["FromReference"]["Name"].ToString() + "§C:" + s["FromReference"]["Code"]["Key"].ToString(),
                s["ToReference"]["Name"].ToString() + "§C:" + s["ToReference"]["Code"]["Key"].ToString(),
                s["Properties"]?.Children<JProperty>().Count() ?? 0))
            .ToArray();

        return result;
    }
}
