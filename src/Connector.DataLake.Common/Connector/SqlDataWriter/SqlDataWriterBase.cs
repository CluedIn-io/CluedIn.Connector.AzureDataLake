using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;

internal abstract class SqlDataWriterBase : ISqlDataWriter
{
    protected const int LoggingThreshold = 1000;
    private static readonly Regex NonAlphaNumericRegex = new("[^a-zA-Z0-9_]");
    protected virtual object GetValue(string key, SqlDataReader reader, IDataLakeJobData configuration)
    {
        return GetValueInternal(key, reader);
    }

    private static object GetValueInternal(string key, SqlDataReader reader)
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
        bool isInitialExport,
        SqlDataReader reader)
    {
        context.Log.LogInformation("Begin writing output.");

        var totalProcessed = await WriteOutputAsync(context, configuration, outputStream, fieldNames, isInitialExport, reader);
        context.Log.LogInformation("End writing output. Total processed: {TotalProcessed}.", totalProcessed);
        return totalProcessed;
    }

    public abstract Task<long> WriteOutputAsync(
        ExecutionContext context,
        IDataLakeJobData configuration,
        Stream outputStream,
        ICollection<string> fieldNames,
        bool isInitialExport,
        SqlDataReader reader);

    protected string EscapeVocabularyKey(string fieldName)
    {
        return NonAlphaNumericRegex.Replace(fieldName, "_");
    }

    protected virtual bool ShouldSkip(IDataLakeJobData configuration, bool isInitialExport, SqlDataReader reader)
    {
        return configuration.IsDeltaMode
            && isInitialExport
            && GetValueInternal(DataLakeConstants.ChangeTypeKey, reader).Equals(VersionChangeType.Removed.ToString());
    }
}
