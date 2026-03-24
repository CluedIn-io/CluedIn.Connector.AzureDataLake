using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common.Extensions;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal class DataLakeStorageFileClient : IStorageFileClient
{
    private readonly DataLakeFileClient _fileClient;

    public DataLakeStorageFileClient(DataLakeFileClient fileClient)
    {
        _fileClient = fileClient ?? throw new ArgumentNullException(nameof(fileClient));
    }

    public async Task<Stream> OpenWriteAsync(bool overwrite, CancellationToken cancellationToken = default)
    {
        return await _fileClient.OpenWriteExAsync(overwrite, cancellationToken: cancellationToken);
    }

    public async Task DeleteIfExistsAsync(CancellationToken cancellationToken = default)
    {
        await _fileClient.DeleteIfExistsAsync(cancellationToken: cancellationToken);
    }

    public async Task SetMetadataAsync(IDictionary<string, string> metadata, CancellationToken cancellationToken = default)
    {
        await _fileClient.SetMetadataAsync(metadata, cancellationToken: cancellationToken);
    }

    public async Task RenameAsync(string targetPath, CancellationToken cancellationToken = default)
    {
        await _fileClient.RenameAsync(targetPath);
    }

    public string Uri => _fileClient.Uri.ToString();

    public string Path => _fileClient.Path;
}
