using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using CluedIn.Core;
using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;

internal interface ISqlDataWriter
{
    Task<long> WriteAsync(
        ExecutionContext context,
        IDataLakeJobData configuration,
        Stream outputStream,
        ICollection<string> fieldNames,
        bool isInitialExport,
        SqlDataReader reader);
}
