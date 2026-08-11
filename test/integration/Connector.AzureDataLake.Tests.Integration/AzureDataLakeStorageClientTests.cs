using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Azure.Storage;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.FileStorage.Common;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

namespace CluedIn.Connector.AzureDataLake.Tests.Integration;

public class AzureDataLakeStorageClientTests
{
    public ITestOutputHelper TestOutputHelper { get; }

    public AzureDataLakeStorageClientTests(ITestOutputHelper testOutputHelper)
    {
        TestOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task GetDataLakeServiceClientAsync_WithSharedKey_CanWrite()
    {
        var connectorConfiguration = CreateConnectorConfigurationWithSharedKey();
        await AssertCanWrite(connectorConfiguration);
    }

    [Fact]
    public async Task GetDataLakeServiceClientAsync_WithSasToken_CanWrite()
    {
        var configuration = CreateBaseConfiguration();
        string sasToken = CreateSasToken();
        configuration[nameof(AzureDataLakeConfigurationConstants.AccountKey)] = sasToken;
        var connectorConfiguration = new AzureDataLakeConnectorConfiguration(configuration);

        await AssertCanWrite(connectorConfiguration);
    }

    private string CreateSasToken()
    {
        var connectorConfiguration = CreateConnectorConfigurationWithSharedKey();
        var sasToken = SasTokenHelper.CreateSasToken(connectorConfiguration.AccountName, connectorConfiguration.AccountKey);
        return sasToken;
    }

    [Fact]
    public async Task GetDataLakeServiceClientAsync_WithServicePrincipal_CanWrite()
    {
        var configuration = CreateBaseConfiguration();
        var tenantId = Environment.GetEnvironmentVariable("ADL2_TENANTID");
        Assert.NotNull(tenantId);
        var clientId = Environment.GetEnvironmentVariable("ADL2_CLIENTID");
        Assert.NotNull(clientId);
        var clientSecret = Environment.GetEnvironmentVariable("ADL2_CLIENTSECRET");
        Assert.NotNull(clientSecret);
        configuration[nameof(AzureDataLakeConfigurationConstants.AuthenticationMethod)] = AuthenticationMethods.ServicePrincipal.ToString();
        configuration[nameof(AzureDataLakeConfigurationConstants.TenantId)] = tenantId;
        configuration[nameof(AzureDataLakeConfigurationConstants.ClientId)] = clientId;
        configuration[nameof(AzureDataLakeConfigurationConstants.ClientSecret)] = clientSecret;
        var connectorConfiguration = new AzureDataLakeConnectorConfiguration(configuration);

        await AssertCanWrite(connectorConfiguration);
    }

    [Fact(Skip = "Workload identity tests are currently skipped until we setup federation using oidc https://learn.microsoft.com/en-us/graph/api/federatedidentitycredential-post?view=graph-rest-1.0&tabs=http")]
    public async Task GetDataLakeServiceClientAsync_WithWorkloadIdentity_CanWrite()
    {
        var configuration = CreateBaseConfiguration();
        var tenantId = Environment.GetEnvironmentVariable("ADL2_TENANTID");
        Assert.NotNull(tenantId);
        var clientId = Environment.GetEnvironmentVariable("ADL2_CLIENTID");
        Assert.NotNull(clientId);

        Environment.SetEnvironmentVariable("AZURE_TENANT_ID", tenantId);
        Environment.SetEnvironmentVariable("AZURE_CLIENT_ID", clientId);
        var token = "dummy"; //GetTokenFromOidc
        var tmpFilePath = Path.GetTempFileName();
        await File.WriteAllTextAsync(tmpFilePath, token);
        Environment.SetEnvironmentVariable("AZURE_FEDERATED_TOKEN_FILE", tmpFilePath);
        configuration[nameof(AzureDataLakeConfigurationConstants.AuthenticationMethod)] = AuthenticationMethods.WorkloadIdentity.ToString();
        var connectorConfiguration = new AzureDataLakeConnectorConfiguration(configuration);

        await AssertCanWrite(connectorConfiguration);
    }

    private async Task AssertCanWrite(AzureDataLakeConnectorConfiguration connectorConfiguration)
    {
        try
        {
            var logger = NullLoggerFactory.Instance.CreateLogger<AzureDataLakeStorageClient>();
            var constants = new Mock<IAzureDataLakeConfigurationConstants>();
            constants.Setup(x => x.WorkloadIdentityAuthenticationMethodEnabledKeyName).Returns("abc");
            constants.Setup(x => x.WorkloadIdentityAuthenticationMethodEnabledDefaultValue).Returns(true);
            using var client = new AzureDataLakeStorageClient(logger, connectorConfiguration, constants.Object);

            var directoryPath = new DirectoryPath(connectorConfiguration.DirectoryName);
            await client.CreateDirectoryIfNotExistsAsync(directoryPath);

            var inputContent = "Hello, Azure Data Lake!";
            var filePath = await WriteFileAsync(client, directoryPath, inputContent);
            var outputContent = await ReadFileAsync(client, filePath);

            Assert.Equal(inputContent, outputContent);

        }
        finally
        {
            await DeleteFileSystemAsync();
        }
    }

    private async Task DeleteFileSystemAsync()
    {
        var connectorConfiguration = CreateConnectorConfigurationWithSharedKey();
        var client = new DataLakeServiceClient(
            new Uri($"https://{connectorConfiguration.AccountName}.dfs.core.windows.net"),
            new StorageSharedKeyCredential(connectorConfiguration.AccountName, connectorConfiguration.AccountKey));
        var fileSystemCLient = client.GetFileSystemClient(connectorConfiguration.FileSystemName);
        await fileSystemCLient.DeleteIfExistsAsync();
    }

    private AzureDataLakeConnectorConfiguration CreateConnectorConfigurationWithSharedKey()
    {
        var configuration = CreateBaseConfiguration();
        var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
        Assert.NotNull(accountKey);
        configuration[nameof(AzureDataLakeConfigurationConstants.AccountKey)] = accountKey;
        var connectorConfiguration = new AzureDataLakeConnectorConfiguration(configuration);
        return connectorConfiguration;
    }

    private Dictionary<string, object> CreateBaseConfiguration()
    {
        var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
        Assert.NotNull(accountName);

        var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
        var directoryName = $"xunit-{DateTime.Now.Ticks}";
        return new Dictionary<string, object>()
        {
            { nameof(AzureDataLakeConfigurationConstants.AccountName), accountName },
            { nameof(AzureDataLakeConfigurationConstants.FileSystemName), fileSystemName },
            { nameof(AzureDataLakeConfigurationConstants.DirectoryName), directoryName },
        };
    }
    static async Task<FilePath> WriteFileAsync(AzureDataLakeStorageClient client, DirectoryPath directoryPath, string content)
    {
        var filePath = directoryPath.GetFilePath($"testfile-{Guid.NewGuid()}.txt");
        var fileClient = await client.GetFileClientAsync(filePath);
        using var stream = await fileClient.OpenWriteAsync(true);
        using var textWriter = new StreamWriter(stream);
        await textWriter.WriteAsync(content);
        return filePath;
    }

    static async Task<string> ReadFileAsync(AzureDataLakeStorageClient client, FilePath filePath)
    {
        var fileClient = await client.GetFileClientAsync(filePath) as DataLakeStorageFileClient;
        using var stream = await fileClient.OpenReadAsync();
        using var textReader = new StreamReader(stream);
        return await textReader.ReadToEndAsync();
    }
}
