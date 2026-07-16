using System;

using CluedIn.Core.Streams.Models;

using ProviderDefinition = CluedIn.Core.Data.Relational.ProviderDefinition;

namespace CluedIn.Connector.FileStorage.Common.Connector;

internal record ExportJobDataBase(
    Guid StreamId,
    StreamModel StreamModel,
    ProviderDefinition ProviderDefinition,
    IStorageConfiguration StorageConfiguration,
    DateTimeOffset AsOfTime,
    string OutputFormat,
    string ContainerName);
