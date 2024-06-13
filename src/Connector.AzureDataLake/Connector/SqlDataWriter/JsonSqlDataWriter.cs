using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using CluedIn.Core;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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

            foreach(var field in fieldNames)
            { 
                await writer.WritePropertyNameAsync(field);
                var value = GetValue(field, reader);
                if (value is JArray jArray)
                {
                    await jArray.WriteToAsync(writer);
                }
                else if (value is JObject jObject)
                {
                    await jObject.WriteToAsync(writer);
                }
                else
                {
                    await writer.WriteValueAsync(value);
                }
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

    protected override object GetValue(string key, SqlDataReader reader)
    {
        var value = base.GetValue(key, reader);
        if (value == null)
        {
            return null;
        }


        if (value is not string stringValue)
        {
            return value;
        }

        try
        {
            var parsed = JToken.Parse(stringValue);
            if (parsed is JArray || parsed is JObject)
            {
                return parsed;
            }

            return stringValue;
        }
        catch (JsonReaderException)
        {
            return value;
        }
    }
}
