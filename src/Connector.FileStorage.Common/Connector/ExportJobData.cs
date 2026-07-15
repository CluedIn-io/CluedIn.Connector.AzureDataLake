using System;

using CluedIn.Core.Streams.Models;

using ProviderDefinition = CluedIn.Core.Data.Relational.ProviderDefinition;

namespace CluedIn.Connector.FileStorage.Common.Connector;

internal record ExportJobData(
    Guid StreamId,
    StreamModel StreamModel,
    ProviderDefinition ProviderDefinition,
    IStorageConfiguration StorageConfiguration,
    DateTimeOffset AsOfTime,
    string OutputFormat,
    string ContainerName,
    string OutputFileName,
    DirectoryPath BaseDirectoryPath,
    DirectoryPath OutputDirectoryPath,
    bool OutputFileExists,
    bool ExistingFileMatchesExpected,
    LastExportedFile? LastExportedFile,
    bool IsInitialExport) : ExportJobDataBase(StreamId,
    StreamModel,
    ProviderDefinition,
    StorageConfiguration,
    AsOfTime,
    OutputFormat,
    ContainerName);

internal record LastExportedFile(string FileName, DateTimeOffset DataTime, long? TotalRows);
