using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using CluedIn.Connector.FabricOpenMirroring.Connector.SqlDataWriter;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;
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
        OpenMirroringStorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(appContext, streamRepository, configurationConstants, storageFactory, dateTimeOffsetProvider)
    {
        DateTimeOffsetProvider = dateTimeOffsetProvider;
    }

    protected override async Task<string> GetOutputFileNameAsync(
        ExecutionContext context,
        ExportJobDataBase exportJobDataBase,
        bool isInitialExport,
        LastExportedFile? lastExportedFile,
        DirectoryPath outputDirectoryPath)
    {
        var outputFormat = exportJobDataBase.OutputFormat;
        if (isInitialExport || lastExportedFile == null)
        {
            return $"{1:D20}.{outputFormat.ToLowerInvariant()}";
        }

        var lastExportedFileName = Path.GetFileNameWithoutExtension(lastExportedFile.FileName);

        var lastCount = long.Parse(lastExportedFileName);
        var newCount = lastCount + 1;
        return $"{newCount:D20}.{outputFormat.ToLowerInvariant()}";
    }

    protected override async Task<LastExportedFile?> GetLastExportedFile(
        ExecutionContext context,
        SqlConnection connection,
        ExportJobDataBase exportJobDataBase,
        IStorageClient storageClient,
        DirectoryPath outputDirectoryPath)
    {
        var lastFile = await base.GetLastExportedFile(context, connection, exportJobDataBase, storageClient, outputDirectoryPath);

        var files = await storageClient.GetFilesInDirectoryAsync(outputDirectoryPath);
        var lastSequenceNumberInFabric = -1L;
        FullyQualifiedFilePath? lastFileInFabric = null;
        foreach (var file in files)
        {
            var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(file.Name);
            if (fileNameWithoutExtension.Length == 20 &&
                long.TryParse(fileNameWithoutExtension, out var fileNumber) &&
                lastSequenceNumberInFabric < fileNumber)
            {
                lastSequenceNumberInFabric = fileNumber;
                lastFileInFabric = file;
            }
        }

        if (lastFileInFabric == null)
        {
            if (lastFile != null)
            {
                context.Log.LogWarning("No files found in the output directory '{OutputDirectoryPath}' for Stream '{StreamId}', but a last exported file was found in the database. This may indicate that files were deleted from the output directory.", outputDirectoryPath, exportJobDataBase.StreamId);
            }

            return null;
        }

        var fileMetadata = await storageClient.GetFileMetadataAsync(new FilePath(lastFileInFabric.Name, outputDirectoryPath));
        if(fileMetadata == null || !TryGetMetadata(fileMetadata.Metadata, out var exportedFileMetadata))
        {
            context.Log.LogError("Failed to get metadata for file '{FileName}' in output directory '{OutputDirectoryPath}' for Stream '{StreamId}'.", lastFileInFabric.Name, outputDirectoryPath, exportJobDataBase.StreamId);
            throw new ApplicationException($"Failed to get metadata for file '{lastFileInFabric.Name}' in output directory '{outputDirectoryPath}' for Stream '{exportJobDataBase.StreamId}'.");
        }

        return new LastExportedFile(lastFileInFabric.Name, exportedFileMetadata.DataTime, null);
    }

    protected override async Task<bool> GetIsInitialExport(
        ExecutionContext context,
        ExportJobDataBase exportJobDataBase,
        IStorageClient storageClient,
        LastExportedFile? lastExportedFile,
        DirectoryPath outputDirectoryPath)
    {
        // if we haven't exported any file yet, then this is definitely the initial export
        if (lastExportedFile == null)
        {
            return true;
        }

        // Check if _metadata.json file exists in the output directory.
        // If it doesn't exist, it means this is the first time we are exporting to this directory, then we can consider this as the initial export
        if (!await storageClient.FileExistsAsync(outputDirectoryPath.GetFilePath("_metadata.json")))
        {
            return true;
        }

        return false;
    }

    private protected override async Task<ShouldSkipResult> ShouldSkipExport(ExecutionContext context, ExportJobData exportJobData, IStorageClient storageClient)
    {
        // instead of checking for existence of target file, we check for last exported file
        if (exportJobData?.LastExportedFile?.DataTime == exportJobData.AsOfTime)
        {
            return new ShouldSkipResult(true, ExportedBeforeReason);
        }

        return new ShouldSkipResult(false, null);
    }

    private protected override bool GetIsEmptyFileAllowed(ExportJobData exportJobData) => false;

    private protected override async Task InitializeBaseDirectoryAsync(
        ExecutionContext context,
        SqlConnection connection,
        ExportJobData exportJobData,
        IStorageClient client)
    {
        await CreatePartnerEventsJsonIfNotExists();

        async Task CreatePartnerEventsJsonIfNotExists()
        {
            if (!await DistributedLockHelper.TryAcquireExclusiveLock(connection, $"{exportJobData.ProviderDefinition}_PartnerEvents", PartnerEventsJsonLockInMilliseconds))
            {
                context.Log.LogDebug("Unable to acquire lock to partner events for ProviderDefinition '{ProviderDefinitionId}'.", exportJobData.ProviderDefinition.Id);
                return;
            }

            await client.CreateDirectoryIfNotExistsAsync(exportJobData.BaseDirectoryPath);
            var fileClient = await client.GetFileClientAsync(exportJobData.BaseDirectoryPath.GetFilePath("_partnerEvents.json"));

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
        ExportJobData exportJobData,
        IStorageClient client)
    {
        await client.CreateDirectoryIfNotExistsAsync(exportJobData.OutputDirectoryPath);
        await EnsureMetadataJsonExists();

        async Task EnsureMetadataJsonExists()
        {
            if (StorageConfigurationConstants.OutputFormats.Csv.Equals(exportJobData.StorageConfiguration.OutputFormat, StringComparison.OrdinalIgnoreCase))
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
            var fileClient = await client.GetFileClientAsync(exportJobData.OutputDirectoryPath.GetFilePath("_metadata.json"));

            if (exportJobData.LastExportedFile == null || !await fileClient.ExistsAsync())
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
            var fileClient = await client.GetFileClientAsync(exportJobData.OutputDirectoryPath.GetFilePath("_metadata.json"));

            if (exportJobData.LastExportedFile == null || !await fileClient.ExistsAsync())
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
        var isFirstFile = exportJobData.LastExportedFile == null;

        if (isRemoved && !isFirstFile)
        {
            // DataLakeConstants.ChangeTypeKey needs to be the last field in the list
            // And it needs to be added only if the file is not the first one
            baseFieldNames.Add(StorageConfigurationConstants.ChangeTypeKey);
        }

        return baseFieldNames;
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
