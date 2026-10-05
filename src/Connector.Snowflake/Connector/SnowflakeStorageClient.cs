using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.Snowflake.Connector;

// Snowflake has no blob storage concept for this connector to drive through
// IStorageClient/IStorageFileClient - the base export pipeline still calls these methods
// for bookkeeping (base directory path for logging, connection verification), but no real
// bytes are ever written through them. The one thing that must be real is connectivity
// verification, so DirectoryExistsAsync/VerifyConnectionAsync/CreateDirectoryIfNotExistsAsync
// run a lightweight live Snowflake query. The real data path to Snowflake is entirely
// through the Snowpipe Streaming REST API inside SnowflakeSnowpipeSqlDataWriter.
internal class SnowflakeStorageClient : IStorageClient
{
    private readonly ILogger<SnowflakeStorageClient> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly SnowflakeConnectorConfiguration _configuration;
    private readonly ISnowflakeApiClient _apiClient;
    private bool _disposed;

    public SnowflakeStorageClient(
        ILogger<SnowflakeStorageClient> logger,
        ILoggerFactory loggerFactory,
        SnowflakeConnectorConfiguration configuration,
        ISnowflakeApiClient apiClient)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _apiClient = apiClient ?? throw new ArgumentNullException(nameof(apiClient));
    }

    public Task<DirectoryPath> GetBaseDirectoryPathAsync()
    {
        return Task.FromResult(new DirectoryPath(_configuration.RootDirectoryPath));
    }

    public async Task CreateDirectoryIfNotExistsAsync(DirectoryPath directoryPath)
    {
        await VerifyConnectionAsync();
    }

    public async Task<bool> DirectoryExistsAsync(DirectoryPath directory)
    {
        try
        {
            await VerifyConnectionAsync();
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to verify Snowflake connectivity for directory check.");
            return false;
        }
    }

    public async Task VerifyConnectionAsync()
    {
        var result = await _apiClient.ExecuteStatementAsync("SELECT 1");
        if (!result.Success)
        {
            throw new InvalidOperationException("Failed to verify Snowflake connection.");
        }
    }

    public Task DeleteDirectoryAsync(DirectoryPath directoryPath)
    {
        return Task.CompletedTask;
    }

    public Task DeleteFileAsync(FilePath filePath)
    {
        return Task.CompletedTask;
    }

    public Task<bool> FileExistsAsync(FilePath filePath)
    {
        return Task.FromResult(false);
    }

    public Task<FileMetadata> GetFileMetadataAsync(FilePath filePath)
    {
        return Task.FromResult<FileMetadata>(null);
    }

    public Task<IEnumerable<FullyQualifiedFilePath>> GetFilesInDirectoryAsync(DirectoryPath directoryPath)
    {
        return Task.FromResult(Enumerable.Empty<FullyQualifiedFilePath>());
    }

    public Task<IStorageFileClient> GetFileClientAsync(FilePath filePath)
    {
        return Task.FromResult<IStorageFileClient>(new SnowflakeStorageFileClient(filePath));
    }

    public Task SaveDataAsync(FilePath filePath, string content, string contentType)
    {
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        (_apiClient as IDisposable)?.Dispose();
        _disposed = true;
    }
}
