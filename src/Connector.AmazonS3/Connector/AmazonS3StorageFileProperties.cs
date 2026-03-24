using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.AmazonS3.Connector;

internal class AmazonS3StorageFileProperties : IStorageFileProperties
{
    public AmazonS3StorageFileProperties(IDictionary<string, string> metadata, DateTimeOffset? lastModified, long? contentLength)
    {
        Metadata = metadata ?? new Dictionary<string, string>();
        LastModified = lastModified;
        ContentLength = contentLength;
    }

    public IDictionary<string, string> Metadata { get; }
    public DateTimeOffset? LastModified { get; }
    public long? ContentLength { get; }
}
