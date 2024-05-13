using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using CluedIn.Core;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

namespace CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter;

internal class JsonSqlDataWriter : SqlDataWriterBase
{
    public override async Task<long> WriteOutputAsync(ExecutionContext context, Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
    {
        using var stringWriter = new StreamWriter(outputStream);
        using var writer = new JsonTextWriter(stringWriter);
        writer.Formatting = Formatting.Indented;

        var totalProcessed = 0L;
        await writer.WriteStartArrayAsync();
        while (await reader.ReadAsync())
        {
            await writer.WriteStartObjectAsync();

            for (var i = 0; i < fieldNames.Count; i++)
            {
                await writer.WritePropertyNameAsync(reader.GetName(i));
                await writer.WriteValueAsync(reader.GetValue(i));
            }

            await writer.WriteEndObjectAsync();
            totalProcessed++;
            if (totalProcessed % LoggingThreshold == 0)
            {
                context.Log.LogDebug("Written {Total} items.", totalProcessed);
            }
        }
        await writer.WriteEndArrayAsync();
        return totalProcessed;
    }
}
