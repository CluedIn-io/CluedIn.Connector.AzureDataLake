using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.DataLake.Common.Connector.SqlDataWriter;
using CluedIn.Connector.FabricOpenMirroring.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Streams;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

internal class OpenMirroringExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    public OpenMirroringExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        OpenMirroringClient dataLakeClient,
        IOpenMirroringConstants dataLakeConstants,
        OpenMirroringJobDataFactory dataLakeJobDataFactory,
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

        // Only increment if previous file is not empty
        // This is because we are deleting empty files
        var newCount = LastExport.TotalRows > 0 ? lastCount + 1 : lastCount;
        return $"{newCount:D20}.{outputFormat.ToLowerInvariant()}";
    }

    private protected override bool GetIsEmptyFileAllowed(ExportJobData exportJobData) => false;

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
            return new OpenMirroringParquetSqlDataWriter();
        }
        return base.GetSqlDataWriter(outputFormat);
    }
}
