using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;

using CsvHelper;
using CsvHelper.Configuration;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter;

internal class CsvSqlDataWriter : SqlDataWriterBase
{
    public override async Task WriteAsync(ExecutionContext context, Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
    {
        context.Log.LogInformation("Begin writing output.");
        using var writer = new StreamWriter(outputStream);

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture);
        using var csv = new CsvWriter(writer, csvConfig);
        foreach (var fieldName in fieldNames)
        {
            csv.WriteField(fieldName);
        }
        await csv.NextRecordAsync();

        int totalProcessed = 0;
        while (await reader.ReadAsync())
        {
            var fieldValues = fieldNames.Select(name => GetValue(name, reader));
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

        context.Log.LogInformation("End writing output. Total processed: {TotalProcessed}.", totalProcessed);
    }
}
