using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CluedIn.Core;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;

internal class CsvSqlDataWriter : SqlDataWriterBase
{
    public override async Task<long> WriteOutputAsync(
        ExecutionContext context,
        IStorageConfiguration configuration,
        Stream outputStream,
        ICollection<string> fieldNames,
        bool isInitialExport,
        SqlDataReader reader)
    {
        context.Log.LogInformation("Begin writing output.");
        await using var writer = new StreamWriter(outputStream);

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture);
        await using var csv = new CsvWriter(writer, csvConfig);
        foreach (var fieldName in fieldNames)
        {
            var fieldNameToUse = GetFieldName(configuration, fieldName);
            csv.WriteField(fieldNameToUse);
        }

        await csv.NextRecordAsync();

        var totalProcessed = 0L;
        while (await reader.ReadAsync())
        {
            if (ShouldSkip(configuration, isInitialExport, reader))
            {
                continue;
            }

            var fieldValues = fieldNames.Select(name => GetValue(name, reader, configuration));
            foreach (var field in fieldValues)
            {
                csv.WriteField(field);
            }

            await csv.NextRecordAsync();
            totalProcessed++;

            if (totalProcessed % LoggingThreshold == 0)
            {
                context.Log.LogDebug("Written {Total} items.", totalProcessed);
            }
        }

        return totalProcessed;
    }

    protected virtual string GetFieldName(IStorageConfiguration configuration, string fieldName)
    {
        return configuration.ShouldEscapeVocabularyKeys ? EscapeVocabularyKey(fieldName) : fieldName;
    }
}
