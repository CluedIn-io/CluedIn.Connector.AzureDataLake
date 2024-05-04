using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using Newtonsoft.Json;

namespace CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter
{
    internal class JsonSqlDataWriter : ISqlDataWriter
    {
        public async Task WriteAsync(Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader)
        {
            using var stringWriter = new StreamWriter(outputStream);
            using var writer = new JsonTextWriter(stringWriter);
            writer.Formatting = Formatting.Indented;

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
            }
            await writer.WriteEndArrayAsync();
        }
    }
}
