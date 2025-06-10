using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CluedIn.Core;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;

internal abstract class SqlDataWriterBase : ISqlDataWriter
{
    protected const int LoggingThreshold = 1000;
    private static readonly Regex NonAlphaNumericRegex = new("[^a-zA-Z0-9_]");
    protected virtual object GetValue(string key, SqlDataReader reader, IDataLakeJobData configuration)
    {
        var value = reader.GetValue(key);

        if (value == DBNull.Value)
        {
            return null;
        }

        return value;
    }

    public async Task<long> WriteAsync(
        ExecutionContext context,
        IDataLakeJobData configuration,
        Stream outputStream,
        ICollection<string> fieldNames,
        SqlDataReader reader)
    {
        context.Log.LogInformation("Begin writing output.");

        var totalProcessed = await WriteOutputAsync(context, configuration, outputStream, fieldNames, reader);
        context.Log.LogInformation("End writing output. Total processed: {TotalProcessed}.", totalProcessed);
        return totalProcessed;
    }

    public abstract Task<long> WriteOutputAsync(
        ExecutionContext context,
        IDataLakeJobData configuration,
        Stream outputStream,
        ICollection<string> fieldNames,
        SqlDataReader reader);

    protected string EscapeVocabularyKey(string fieldName)
    {
        return NonAlphaNumericRegex.Replace(fieldName, "_");
    }
}
