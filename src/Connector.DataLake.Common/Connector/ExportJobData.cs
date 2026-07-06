using System;

using CluedIn.Core.Streams.Models;

using ProviderDefinition = CluedIn.Core.Data.Relational.ProviderDefinition;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal record ExportJobData(
    Guid StreamId,
    StreamModel StreamModel,
    ProviderDefinition ProviderDefinition,
    IDataLakeJobData StorageConfiguration,
    DateTimeOffset AsOfTime,
    string OutputFormat,
    string ContainerName,
    string OutputFileName,
    string SubDirectory,
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
