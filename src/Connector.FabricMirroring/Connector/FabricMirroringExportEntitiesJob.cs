using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams;

using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;

using Newtonsoft.Json;

using static CluedIn.Connector.DataLake.Common.DataLakeConstants;

namespace CluedIn.Connector.FabricMirroring.Connector;

internal class FabricMirroringExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public FabricMirroringExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        FabricMirroringClient dataLakeClient,
        IFabricMirroringConstants dataLakeConstants,
        FabricMirroringJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, dataLakeClient, dataLakeConstants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    protected override async Task<string> GetDefaultOutputFileNameAsync(ExecutionContext context, IDataLakeJobData configuration, Guid streamId, string containerName, DateTimeOffset asOfTime, string outputFormat)
    {
        if (LastExport == null)
        {
            return $"{1:D20}.{outputFormat.ToLowerInvariant()}";
        }


        var lastName = Path.GetFileNameWithoutExtension(LastExport.FilePath);
        var lastCount = int.Parse(lastName);

        var newCount = lastCount + 1;
        return $"{newCount:D20}.{outputFormat.ToLowerInvariant()}";

        //return await base.GetDefaultOutputFileNameAsync(context, configuration, streamId, containerName, asOfTime, outputFormat);
    }

    protected override async Task InitializeDirectoryAsync(IDataLakeJobData configuration, Guid streamId, DataLakeDirectoryClient directoryClient)
    {
        var fileClient = directoryClient.GetFileClient("_metadata.json");

        if (!await fileClient.ExistsAsync())
        {
            await using var outputStream = await fileClient.OpenWriteAsync(true);
            await outputStream.WriteAsync(Encoding.UTF8.GetBytes(
                $$"""
                {
                   "keyColumns": ["Id"]
                }
                """));
            await outputStream.FlushAsync();
        }
    }

    private protected override async Task<ExportHistory> GetLastExport(ExecutionContext context, SqlConnection connection, Guid streamId, IDataLakeJobData configuration)
    {
        var subDirectory = await GetSubDirectory(configuration, streamId);
        if (!await _dataLakeClient.FileInPathExists(configuration, "_metadata.json", subDirectory))
        {
            return null;
        }

        return await base.GetLastExport(context, connection, streamId, configuration);
    }

    protected override Task<string> GetSubDirectory(IDataLakeJobData configuration, Guid streamId)
    {
        return Task.FromResult(streamId.ToString("N"));
    }

    protected override ISqlDataWriter GetSqlDataWriter(string outputFormat)
    {
        var format = outputFormat.Trim();
        if (format.Equals(DataLakeConstants.OutputFormats.Parquet, StringComparison.OrdinalIgnoreCase))
        {
            return new ParquetSqlDataWriter(new FabricDataTransformer());
        }
        return base.GetSqlDataWriter(outputFormat);
    }
}

internal class FabricDataTransformer : IDataTransformer
{
    public KeyValuePair<string, Type> GetType(KeyValuePair<string, Type> pair)
    {
        if (pair.Key == "__ChangeType__")
        {
            return new KeyValuePair<string, Type>("__rowMarker__", typeof(string));
        }

        if (pair.Value == typeof(IEnumerable<string>))
        {
            return new KeyValuePair<string, Type>(pair.Key, typeof(string));
        }

        return pair;
    }

    public KeyValuePair<string, object> Transform(KeyValuePair<string, object> pair)
    {
        /*
            0 = INSERT
            1 = UPDATE
            2 = DELETE
            3 = UPSERT
         */
        if (pair.Key == "__ChangeType__")
        {
            var changeType = Enum.Parse<VersionChangeType>(pair.Value as string);
            var value = changeType switch
            {
                VersionChangeType.Removed => 2,
                _ => 3,
            };
            return new KeyValuePair<string, object>("__rowMarker__", value.ToString());
        }

        if (pair.Value is IEnumerable<string>)
        {
            return new KeyValuePair<string, object>(pair.Key, JsonConvert.SerializeObject(pair.Value));
        }

        return pair;
    }
}
