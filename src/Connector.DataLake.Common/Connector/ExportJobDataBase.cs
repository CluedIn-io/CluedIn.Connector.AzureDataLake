using System;

using CluedIn.Core.Streams.Models;

using ProviderDefinition = CluedIn.Core.Data.Relational.ProviderDefinition;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal record ExportJobDataBase(
    Guid StreamId,
    StreamModel StreamModel,
    ProviderDefinition ProviderDefinition,
    IDataLakeJobData StorageConfiguration,
    DateTimeOffset AsOfTime,
    string OutputFormat,
    string ContainerName);
