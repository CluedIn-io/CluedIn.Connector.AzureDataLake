using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

using Azure;

using CluedIn.Connector.FabricOpenMirroring.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

namespace CluedIn.Connector.FabricOpenMirroring.Tests.Integration;

public class OpenMirroringStorageClientTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public OpenMirroringStorageClientTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task UpdateOrCreateMirroredDatabaseAsync_WhenShouldCreateMirroredDatabaseIsFalse_Skips()
    {
        // Arrange
        var configuration = CreateConfiguration(shouldCreateMirroredDatabase: false);
        var client = CreateClient(configuration);

        // Act
        var result = await client.UpdateOrCreateMirroredDatabaseAsync(Guid.NewGuid(), isEnabled: true);

        // Assert
        Assert.True(result.IsSkipped);
        Assert.False(result.IsCreated);
        Assert.Null(result.IsEnabled);
    }

    [Fact]
    public async Task UpdateOrCreateMirroredDatabaseAsync_WhenEnabled_CreatesAndStartsMirroring()
    {
        // Arrange
        var configuration = CreateConfiguration(shouldCreateMirroredDatabase: true, mirroredDatabaseNameOverride: $"DB_{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}_{Guid.NewGuid():N}");
        var client = CreateClient(configuration);
        var providerDefinitionId = Guid.NewGuid();

        // Act
        var result = await client.UpdateOrCreateMirroredDatabaseAsync(providerDefinitionId, isEnabled: true);

        // Assert
        Assert.False(result.IsSkipped);
        Assert.True(result.IsCreated);
        Assert.True(result.IsEnabled);
    }

    [Fact]
    public async Task UpdateOrCreateMirroredDatabaseAsync_WhenDisabled_CreatesAndStopsMirroring()
    {
        // Arrange
        var configuration = CreateConfiguration(shouldCreateMirroredDatabase: true, mirroredDatabaseNameOverride: $"DB_{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}_{Guid.NewGuid():N}");
        var client = CreateClient(configuration);
        var providerDefinitionId = Guid.NewGuid();

        // Act
        var result = await client.UpdateOrCreateMirroredDatabaseAsync(providerDefinitionId, isEnabled: false);

        // Assert
        Assert.False(result.IsSkipped);
        Assert.True(result.IsCreated);
        Assert.False(result.IsEnabled);
    }

    [Fact]
    public async Task HasValidWorkspaceAsync_WhenWorkspaceNameHasWhitespace_StillFindsWorkspace()
    {
        // Arrange - add whitespace around workspace name to verify trimming
        var workspaceName = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_WORKSPACENAME");
        Assert.NotNull(workspaceName);

        var configuration = CreateConfiguration(
            shouldCreateMirroredDatabase: false,
            workspaceNameOverride: $"  {workspaceName}  ");
        var client = CreateClient(configuration);

        // Act
        var result = await client.HasValidWorkspaceAsync();

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task HasValidWorkspaceAsync_WhenWorkspaceNameIsValid_ReturnsTrue()
    {
        // Arrange
        var configuration = CreateConfiguration(shouldCreateMirroredDatabase: false);
        var client = CreateClient(configuration);

        // Act
        var result = await client.HasValidWorkspaceAsync();

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task HasValidWorkspaceAsync_WhenWorkspaceNameIsInvalid_ThrowsException()
    {
        // Arrange
        var configuration = CreateConfiguration(
            shouldCreateMirroredDatabase: false,
            workspaceNameOverride: "NonExistentWorkspace_" + Guid.NewGuid());
        var client = CreateClient(configuration);

        // Act & Assert
        await Assert.ThrowsAsync<RequestFailedException>(client.HasValidWorkspaceAsync);
    }

    private OpenMirroringStorageClient CreateClient(OpenMirroringConnectorConfiguration configuration)
    {
        var applicationContext = new ApplicationContext(new Castle.Windsor.WindsorContainer());
        var mockDateTimeOffsetProvider = new Mock<IDateTimeOffsetProvider>();
        mockDateTimeOffsetProvider.Setup(x => x.GetCurrentUtcTime()).Returns(() => DateTimeOffset.UtcNow);

        return new OpenMirroringStorageClient(
            NullLogger<OpenMirroringStorageClient>.Instance,
            configuration,
            applicationContext,
            mockDateTimeOffsetProvider.Object);
    }

    private OpenMirroringConnectorConfiguration CreateConfiguration(
        bool shouldCreateMirroredDatabase,
        string workspaceNameOverride = null,
        string mirroredDatabaseNameOverride = null)
    {
        var tenantId = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_TENANTID");
        var clientId = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_CLIENTID");
        var clientSecretEncoded = Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_CLIENTSECRET");
        var workspaceName = workspaceNameOverride ?? Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_WORKSPACENAME");
        var mirroredDatabaseName = mirroredDatabaseNameOverride ?? Environment.GetEnvironmentVariable("FABRICOPENMIRRORING_MIRROREDDATABASENAME");

        Assert.NotNull(tenantId);
        Assert.NotNull(clientId);
        Assert.NotNull(clientSecretEncoded);
        Assert.NotNull(workspaceName);
        Assert.NotNull(mirroredDatabaseName);

        var clientSecretString = Encoding.UTF8.GetString(Convert.FromBase64String(clientSecretEncoded));
        Assert.False(string.IsNullOrWhiteSpace(clientSecretString));

        _testOutputHelper.WriteLine(
            "Using TenantId: '{0}', ClientId: '{1}', WorkspaceName: '{2}', MirroredDatabaseName: '{3}', ShouldCreateMirroredDatabase: '{4}'.",
            tenantId, clientId, workspaceName, mirroredDatabaseName, shouldCreateMirroredDatabase);

        var dict = new Dictionary<string, object>
        {
            { nameof(OpenMirroringConfigurationConstants.TenantId), tenantId },
            { nameof(OpenMirroringConfigurationConstants.ClientId), clientId },
            { nameof(OpenMirroringConfigurationConstants.ClientSecret), clientSecretString },
            { nameof(OpenMirroringConfigurationConstants.WorkspaceName), workspaceName },
            { nameof(OpenMirroringConfigurationConstants.MirroredDatabaseName), mirroredDatabaseName },
            { nameof(OpenMirroringConfigurationConstants.ShouldCreateMirroredDatabase), shouldCreateMirroredDatabase },
        };

        if (shouldCreateMirroredDatabase)
        {
            var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("INTEGRATIONTEST_STREAMCACHE");
            Assert.NotNull(streamCacheConnectionStringEncoded);
            var streamCacheConnectionString = Encoding.UTF8.GetString(Convert.FromBase64String(streamCacheConnectionStringEncoded));
            dict[nameof(StorageConfigurationConstants.StreamCacheConnectionString)] = streamCacheConnectionString;
            dict[nameof(StorageConfigurationConstants.IsStreamCacheEnabled)] = true;
        }

        return new OpenMirroringConnectorConfiguration(dict);
    }

    [Fact]
    public async Task UpdateOrCreateMirroredDatabaseAsync_WhenLockAlreadyHeld_SkipsCreation()
    {
        // Arrange
        var configuration = CreateConfiguration(shouldCreateMirroredDatabase: true);
        var client = CreateClient(configuration);
        var providerDefinitionId = Guid.NewGuid();

        // Acquire the same lock that UpdateOrCreateMirroredDatabaseAsync would try to acquire
        var tcs = new TaskCompletionSource();

        var lockTask = AcquireLock(configuration, providerDefinitionId, TimeSpan.FromSeconds(5), tcs);
        var createTask = UpdateOrCreateDatabase(client, providerDefinitionId, tcs);
        await Task.WhenAll(lockTask, createTask);

        var result = await createTask;

        // Assert
        Assert.True(result.IsSkipped);
        Assert.False(result.IsCreated);
        Assert.Null(result.IsEnabled);

        static async Task AcquireLock(OpenMirroringConnectorConfiguration configuration, Guid providerDefinitionId, TimeSpan lockHoldTime, TaskCompletionSource taskCompletionSource)
        {
            using var transactionScope = new TransactionScope(
                        TransactionScopeOption.Required,
                        lockHoldTime,
                        TransactionScopeAsyncFlowOption.Enabled);
            await using var connection = new SqlConnection(configuration.StreamCacheConnectionString);
            await connection.OpenAsync();

            var lockAcquired = await DistributedLockHelper.TryAcquireExclusiveLock(
                connection,
                $"CreateMirroredDatabase_{providerDefinitionId}",
                timeOutInMilliseconds: 1000);
            Assert.True(lockAcquired, "Test setup failed: could not acquire the lock.");
            taskCompletionSource.SetResult(); // Signal that the lock has been acquired
            await Task.Delay(lockHoldTime);
        }

        static async Task<UpdateOrCreateMirroredDatabaseResult> UpdateOrCreateDatabase(OpenMirroringStorageClient client, Guid providerDefinitionId, TaskCompletionSource taskCompletionSource)
        {

            // Act
            await taskCompletionSource.Task; // Wait for the lock to be held
            return await client.UpdateOrCreateMirroredDatabaseAsync(providerDefinitionId, isEnabled: true);
        }
    }
}
