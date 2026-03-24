using System;
using System.Collections.Generic;

using Azure.Storage.Files.DataLake.Models;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal class DataLakeStorageFileProperties : IStorageFileProperties
{
    private readonly PathProperties _pathProperties;

    public DataLakeStorageFileProperties(PathProperties pathProperties)
    {
        _pathProperties = pathProperties ?? throw new ArgumentNullException(nameof(pathProperties));
    }

    public IDictionary<string, string> Metadata => _pathProperties.Metadata;
    public DateTimeOffset? LastModified => _pathProperties.LastModified;
    public long? ContentLength => _pathProperties.ContentLength;
}
