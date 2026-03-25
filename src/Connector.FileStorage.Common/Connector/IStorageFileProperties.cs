using System;
using System.Collections.Generic;

namespace CluedIn.Connector.DataLake.Common.Connector;

public interface IStorageFileProperties
{
    IDictionary<string, string> Metadata { get; }
    DateTimeOffset? LastModified { get; }
    long? ContentLength { get; }
}
