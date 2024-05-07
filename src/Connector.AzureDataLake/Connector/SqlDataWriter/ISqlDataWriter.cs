using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using CluedIn.Core;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.AzureDataLake.Connector.SqlDataWriter
{
    internal interface ISqlDataWriter
    {
        Task WriteAsync(ExecutionContext context, Stream outputStream, ICollection<string> fieldNames, SqlDataReader reader);
    }
}
