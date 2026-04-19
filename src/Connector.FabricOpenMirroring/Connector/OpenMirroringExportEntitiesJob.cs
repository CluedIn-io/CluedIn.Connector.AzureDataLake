using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;
using CluedIn.Connector.FabricOpenMirroring.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Streams;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

internal class OpenMirroringExportEntitiesJob : StorageExportEntitiesJobBase
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
        IOpenMirroringConfigurationConstants configurationConstants,
        OpenMirroringStorageFactory storageStorageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, configurationConstants, storageStorageFactory, dateTimeOffsetProvider)
    {
        DateTimeOffsetProvider = dateTimeOffsetProvider;
    }

    protected override async Task<string> GetDefaultOutputFileNameAsync(ExecutionContext context, IStorageConfiguration configuration, Guid streamId, string containerName, DateTimeOffset asOfTime, string outputFormat)
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
    private protected override async Task InitializeBaseDirectoryAsync(
        ExecutionContext context,
        SqlConnection connection,
        IStorageConfiguration configuration,
        ExportJobData exportJobData,
        IStorageClient client,
        DirectoryPath baseDirectoryPath)
    {
        await CreatePartnerEventsJsonIfNotExists();

        async Task CreatePartnerEventsJsonIfNotExists()
        {
            if (!await DistributedLockHelper.TryAcquireExclusiveLock(connection, $"{exportJobData.ProviderDefinition}_PartnerEvents", PartnerEventsJsonLockInMilliseconds))
            {
                context.Log.LogDebug("Unable to acquire lock to partner events for ProviderDefinition '{ProviderDefinitionId}'.", exportJobData.ProviderDefinition.Id);
                return;
            }

            await client.CreateDirectoryIfNotExists(baseDirectoryPath);
            var fileClient = await client.GetFileClient(baseDirectoryPath.GetFilePath("_partnerEvents.json"));

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
    private protected override async Task InitializeOutputDirectoryAsync(
        ExecutionContext context,
        SqlConnection connection,
        IStorageConfiguration configuration,
        ExportJobData exportJobData,
        IStorageClient client,
        DirectoryPath outputDirectoryPath)
    {
        await client.CreateDirectoryIfNotExists(outputDirectoryPath);
        await EnsureMetadataJsonExists();

        async Task EnsureMetadataJsonExists()
        {
            if (StorageConfigurationConstants.OutputFormats.Csv.Equals(configuration.OutputFormat, StringComparison.OrdinalIgnoreCase))
            {
                await EnsureCsvMetadataJsonExists();
            }
            else
            {
                await EnsureGenericMetadataJsonExists();
            }
        }

        async Task EnsureCsvMetadataJsonExists()
        {
            var fileClient = await client.GetFileClient(outputDirectoryPath.GetFilePath("_metadata.json"));

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

        async Task EnsureGenericMetadataJsonExists()
        {
            var fileClient = await client.GetFileClient(outputDirectoryPath.GetFilePath("_metadata.json"));

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
    }

    private protected override async Task<List<string>> GetFieldNamesAsync(
        ExecutionContext context,
        ExportJobData exportJobData,
        IStorageConfiguration configuration,
        List<string> fieldNames)
    {
        var baseFieldNames = await base.GetFieldNamesAsync(context, exportJobData, configuration, fieldNames);

        // We need to make sure DataLakeConstants.ChangeTypeKey is the last field in the list (if we need it)
        var isRemoved = baseFieldNames.Remove(StorageConfigurationConstants.ChangeTypeKey);
        var isFirstFile = LastExport == null;

        if (isRemoved && !isFirstFile)
        {
            // DataLakeConstants.ChangeTypeKey needs to be the last field in the list
            // And it needs to be added only if the file is not the first one
            baseFieldNames.Add(StorageConfigurationConstants.ChangeTypeKey);
        }

        return baseFieldNames;
    }

    private protected override async Task<ExportHistory> GetLastExport(
        ExecutionContext context,
        SqlConnection connection,
        IStorageConfiguration configuration,
        ExportJobDataBase exportJobData,
        DirectoryPath outputDirectoryPath)
    {
        var client = await CreateStorageClient(context, configuration);
        if (!await client.FileExists(outputDirectoryPath.GetFilePath("_metadata.json")))
        {
            return null;
        }

        return await base.GetLastExport(context, connection, configuration, exportJobData, outputDirectoryPath);
    }

    private protected override Task<string> GetOutputDirectoryNameAsync(ExecutionContext executionContext, IStorageConfiguration configuration, ExportJobDataBase exportJobData)
    {
        return OutputDirectoryHelper.GetSubDirectory(
            executionContext,
            configuration,
            exportJobData.StreamModel.Id,
            exportJobData.StreamModel.ContainerName,
            exportJobData.AsOfTime,
            exportJobData.OutputFormat);
    }

    protected override ISqlDataWriter GetSqlDataWriter(string outputFormat)
    {
        var format = outputFormat.Trim();
        if (format.Equals(StorageConfigurationConstants.OutputFormats.Csv, StringComparison.OrdinalIgnoreCase))
        {
            return new OpenMirroringCsvSqlDataWriter();
        }
        if (format.Equals(StorageConfigurationConstants.OutputFormats.Parquet, StringComparison.OrdinalIgnoreCase))
        {
            return new OpenMirroringParquetSqlDataWriter();
        }
        return base.GetSqlDataWriter(outputFormat);
    }
}
