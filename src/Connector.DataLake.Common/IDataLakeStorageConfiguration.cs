using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.DataLake.Common;

internal interface IDataLakeStorageConfiguration : IStorageConfiguration
{
    string StorageUri { get; }
}
