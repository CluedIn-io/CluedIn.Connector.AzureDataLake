using System;

using Azure.Storage.Files.DataLake;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal class DataLakeStorageDirectoryClient : IStorageDirectoryClient
{
    private readonly DataLakeDirectoryClient _directoryClient;

    public DataLakeStorageDirectoryClient(DataLakeDirectoryClient directoryClient)
    {
        _directoryClient = directoryClient ?? throw new ArgumentNullException(nameof(directoryClient));
    }

    public IStorageFileClient GetFileClient(string fileName)
    {
        var fileClient = _directoryClient.GetFileClient(fileName);
        return new DataLakeStorageFileClient(fileClient);
    }
}
