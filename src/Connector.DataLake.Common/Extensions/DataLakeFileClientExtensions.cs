using Azure.Storage.Files.DataLake;
using Azure;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Files.DataLake.Models;

namespace CluedIn.Connector.DataLake.Common.Extensions;

public static class DataLakeFileClientExtensions
{
    /// <summary>
    /// Opens a stream for writing to the Data Lake file, safely handling the case where 
    /// OpenWriteAsync fails with a 404 because the file does not yet exist.
    /// This pattern is necessary due to the DataLake SDK's internal GetProperties check.
    /// </summary>
    /// <param name="fileClient">The DataLakeFileClient instance.</param>
    /// <param name="overwrite">Whether to overwrite the file if it exists.</param>
    /// <param name="options">Optional parameters for the write operation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A writable Stream for the file.</returns>
    public static async Task<Stream> OpenWriteExAsync(
        this DataLakeFileClient fileClient,
        bool overwrite,
        DataLakeFileOpenWriteOptions options = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // 1. Attempt the standard OpenWriteAsync call.
            return await fileClient.OpenWriteAsync(overwrite, options, cancellationToken);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // This is the specific error when GetPropertiesAsync fails because the file doesn't exist.

            await fileClient.CreateAsync(cancellationToken: cancellationToken);

            // Retry the OpenWriteAsync call. This time, GetPropertiesAsync should succeed 
            // (because the file exists now), and the stream will be opened.
            // We pass the original 'overwrite' flag here to handle the stream logic correctly.
            return await fileClient.OpenWriteAsync(overwrite, options, cancellationToken);
        }
    }
}
