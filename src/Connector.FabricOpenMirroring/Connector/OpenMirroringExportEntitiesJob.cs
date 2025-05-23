using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
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
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

internal class OpenMirroringExportEntitiesJob : DataLakeExportEntitiesJobBase
{
    private const int PartnerEventsJsonLockInMilliseconds = 100;
    private static readonly AssemblyName _connectorAssemblyName = typeof(OpenMirroringExportEntitiesJob).Assembly.GetName();
    private static readonly AssemblyName _cluedInCoreAssemblyName = typeof(IDateTimeOffsetProvider).Assembly.GetName();
    private static readonly FileVersionInfo _connectorFileVersionInfo = FileVersionInfo.GetVersionInfo(typeof(OpenMirroringExportEntitiesJob).Assembly.Location);
    private static readonly FileVersionInfo _cluedInCoreFileVersionInfo = FileVersionInfo.GetVersionInfo(typeof(IDateTimeOffsetProvider).Assembly.Location);
    private static readonly string _partnerName = "CluedIn ApS";

    private IDateTimeOffsetProvider DateTimeOffsetProvider { get; }

    public OpenMirroringExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        OpenMirroringClient dataLakeClient,
        IOpenMirroringConstants dataLakeConstants,
        OpenMirroringJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, dataLakeClient, dataLakeConstants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
        DateTimeOffsetProvider = dateTimeOffsetProvider;
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

    private protected override bool ShouldSkipExport(ExportJobData exportJobData)
    {
        return LastExport?.DataTime == exportJobData.AsOfTime;
    }

    private protected override bool GetIsEmptyFileAllowed(ExportJobData exportJobData) => false;

    private protected override async Task InitializeDirectoryAsync(ExecutionContext context, SqlConnection connection, IDataLakeJobData configuration, ExportJobData exportJobData, DataLakeDirectoryClient directoryClient)
    {
        await EnsureMetadataJsonExists(directoryClient);
        await CreatePartnerEventsJsonIfNotExists();

        async Task EnsureMetadataJsonExists(DataLakeDirectoryClient directoryClient)
        {
            if (DataLakeConstants.OutputFormats.Csv.Equals(configuration.OutputFormat, StringComparison.OrdinalIgnoreCase))
            {
                await EnsureCsvMetadataJsonExists(directoryClient);
            }
            else
            {
                await EnsureGenericMetadataJsonExists(directoryClient);
            }
        }

        async Task EnsureCsvMetadataJsonExists(DataLakeDirectoryClient directoryClient)
        {
            var fileClient = directoryClient.GetFileClient("_metadata.json");

            if (IsInitialExport || !await fileClient.ExistsAsync())
            {
                await using var outputStream = await fileClient.OpenWriteAsync(true);
                await outputStream.WriteAsync(Encoding.UTF8.GetBytes(
                    $$"""
                    {
                       "keyColumns": ["Id"],
                       "fileExtension": "csv",
                       "fileFormat": "csv",
                       "fileFormatTypeProperties": {
                           "firstRowAsHeader": true,
                           "rowSeparator": "\r\n",
                           "columnSeparator": ",",
                           "quoteCharacter": "\"",
                           "escapeCharacter": "\"",
                           "nullValue": "",
                           "encoding": "UTF-8"
                       }
                    }
                    """));
                await outputStream.FlushAsync();
            }
        }

        async Task EnsureGenericMetadataJsonExists(DataLakeDirectoryClient directoryClient)
        {
            var fileClient = directoryClient.GetFileClient("_metadata.json");

            if (IsInitialExport || !await fileClient.ExistsAsync())
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

        async Task CreatePartnerEventsJsonIfNotExists()
        {
            if (!await DistributedLockHelper.TryAcquireExclusiveLock(connection, $"{exportJobData.ProviderDefinition}_PartnerEvents", PartnerEventsJsonLockInMilliseconds))
            {
                context.Log.LogDebug("Unable to acquire lock to partner events for ProviderDefinition '{ProviderDefinitionId}'.", exportJobData.ProviderDefinition.Id);
                return;
            }
            var landingZoneDirectoryClient = await _dataLakeClient.EnsureDataLakeDirectoryExist(configuration);
            var fileClient = landingZoneDirectoryClient.GetFileClient("_partnerEvents.json");

            if (!await fileClient.ExistsAsync())
            {
                await using var outputStream = await fileClient.OpenWriteAsync(true);
                await outputStream.WriteAsync(Encoding.UTF8.GetBytes(
                $$"""
                {
                  "partnerName": "{{_partnerName}}",
                  "sourceInfo": {
                    "sourceType": "{{_connectorAssemblyName.Name}}",
                    "sourceVersion": "{{_connectorAssemblyName.Version}}",
                    "additionalInformation": {
                      "sourceFileVersion": "{{_connectorFileVersionInfo.ProductVersion}}",
                      "cluedInServerVersion": "{{_cluedInCoreAssemblyName.Version}}" ,
                      "cluedInServerFileVersion": "{{_cluedInCoreFileVersionInfo.ProductVersion}}",
                      "organizationId": "{{exportJobData.StreamModel.OrganizationId:N}}",
                      "providerDefinitionId": "{{exportJobData.ProviderDefinition.Id:N}}",
                      "createdAt": "{{DateTimeOffsetProvider.GetCurrentUtcTime().ToString("o")}}"
                    }
                  }
                }
                """));
                await outputStream.FlushAsync();
            }
        }
    }

    private protected override async Task<List<string>> GetFieldNamesAsync(
        ExecutionContext context,
        ExportJobData exportJobData,
        IDataLakeJobData configuration,
        List<string> fieldNames)
    {
        var baseFieldNames = await base.GetFieldNamesAsync(context, exportJobData, configuration, fieldNames);

        // We need to make sure DataLakeConstants.ChangeTypeKey is the last field in the list (if we need it)
        var isRemoved = baseFieldNames.Remove(DataLakeConstants.ChangeTypeKey);
        var isFirstFile = LastExport == null;

        if (isRemoved && !isFirstFile)
        {
            // DataLakeConstants.ChangeTypeKey needs to be the last field in the list
            // And it needs to be added only if the file is not the first one
            baseFieldNames.Add(DataLakeConstants.ChangeTypeKey);
        }

        return baseFieldNames;
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
        if (format.Equals(DataLakeConstants.OutputFormats.Csv, StringComparison.OrdinalIgnoreCase))
        {
            return new OpenMirroringCsvSqlDataWriter();
        }
        if (format.Equals(DataLakeConstants.OutputFormats.Parquet, StringComparison.OrdinalIgnoreCase))
        {
            return new OpenMirroringParquetSqlDataWriter();
        }
        return base.GetSqlDataWriter(outputFormat);
    }
}
