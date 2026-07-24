using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Azure.Storage;
using Azure.Storage.Files.DataLake;

using Castle.Windsor;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.DataLake.Common.Tests.Integration;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

namespace CluedIn.Connector.AzureDataLake.Tests.Integration;

public class AzureDataLakeConnectorTests : DataLakeConnectorTestsBase<AzureDataLakeConnector, AzureDataLakeStorageFactory, IAzureDataLakeConfigurationConstants>
{
    protected override Guid StorageProviderId => AzureDataLakeConfigurationConstants.DataLakeProviderId;

    public AzureDataLakeConnectorTests(ITestOutputHelper testOutputHelper)
        : base(testOutputHelper)
    {
    }

    [Fact]
    public async Task VerifyConnection_WhenValid_ReturnsSuccess()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.True(result.Success);
        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    [Fact]
    public async Task VerifyConnection_WhenValidSasToken_ReturnsSuccess()
    {
        var configuration = CreateConfigurationWithoutStreamCache();

        var sasToken = SasTokenHelper.CreateSasToken(
            configuration[nameof(AzureDataLakeConfigurationConstants.AccountName)] as string,
            configuration[nameof(AzureDataLakeConfigurationConstants.AccountKey)] as string);
        configuration[nameof(AzureDataLakeConfigurationConstants.AccountKey)] = sasToken;

        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            setupResult.DateTimeOffsetProviderMock.Setup(x => x.GetCurrentUtcTime()).Returns(DateTimeOffset.UtcNow);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.True(result.Success);
        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    private void DeleteFileSystemIfExists(AzureDataLakeConnectorConfiguration jobData)
    {
        var client = GetDataLakeClient(jobData);
        client.GetFileSystemClient(jobData.FileSystemName).DeleteIfExists();
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    [InlineData("ALLCAPS")]
    [InlineData("someCaps")]
    [InlineData("some space")]
    [InlineData("1 2")]
    public async Task VerifyConnection_WhenInvalidAccountName_ReturnInvalidAccountNameErrorMessage(string accountName)
    {

        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AzureDataLakeConfigurationConstants.AccountName)] = accountName;
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);
        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.Equal(AzureDataLakeConnector.InvalidAccountNameErrorMessage, result.ErrorMessage);
        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    [Theory]
    [InlineData("1")]
    public async Task VerifyConnection_WhenInexistentAccountName_ReturnInvalidAccountNameErrorMessage(string accountName)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AzureDataLakeConfigurationConstants.AccountName)] = accountName;
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.Equal(AzureDataLakeConnector.InvalidCredentialsErrorMessage, result.ErrorMessage);
        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    [InlineData("notbase64")]
    public async Task VerifyConnection_WhenInvalidAccountKey_ReturnInvalidAccountKeyErrorMessage(string accountKey)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AzureDataLakeConfigurationConstants.AccountKey)] = accountKey;
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.Equal(AzureDataLakeConnector.InvalidAccountKeyErrorMessage, result.ErrorMessage);

        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    [Theory]
    [InlineData("sv=2025-07-05&ss=b&srt=co&spr=https&st=2099-07-23T07%3A33%3A38Z&se=2026-07-24T07%3A33%3A38Z&sp=rwdlc&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=co&spr=https&st=2000-07-23T07%3A33%3A38Z&se=2000-07-24T07%3A33%3A38Z&sp=rwdlc&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=co&spr=https&st=2026-07-22T07%3A33%3A38Z&se=2026-07-23T07%3A33%3A38Z&sp=rwdlc&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=co&spr=https&st=asdf&se=2026-07-23T07%3A33%3A38Z&sp=rwdlc&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=co&spr=https&st=2026-07-22T07%3A33%3A38Z&se=asdf&sp=rwdlc&sig=dummysignature")]
    public async Task VerifyConnection_WhenInvalidSasTokenTime_ReturnInvalidSasTokenErrorMessage(string sasToken)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AzureDataLakeConfigurationConstants.AccountKey)] = sasToken;
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.Equal(AzureDataLakeConnector.InvalidSasTokenTimeErrorMessage, result.ErrorMessage);

        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    [Theory]
    [InlineData("sv=2025-07-05&ss=tqf&srt=co&spr=https&st=2000-01-01T07%3A33%3A38Z&se=2099-12-31T07%3A33%3A38Z&sp=rwdlc&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=sc&spr=https&st=2000-01-01T07%3A33%3A38Z&se=2099-12-31T07%3A33%3A38Z&sp=rwdlc&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=so&spr=https&st=2000-01-01T07%3A33%3A38Z&se=2099-12-31T07%3A33%3A38Z&sp=rwdlc&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=s&spr=https&st=2000-01-01T07%3A33%3A38Z&se=2099-12-31T07%3A33%3A38Z&sp=rwdlc&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=sc&spr=https&st=2000-01-01T07%3A33%3A38Z&se=2099-12-31T07%3A33%3A38Z&sp=r&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=sc&spr=https&st=2000-01-01T07%3A33%3A38Z&se=2099-12-31T07%3A33%3A38Z&sp=rw&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=sc&spr=https&st=2000-01-01T07%3A33%3A38Z&se=2099-12-31T07%3A33%3A38Z&sp=rwd&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=sc&spr=https&st=2000-01-01T07%3A33%3A38Z&se=2099-12-31T07%3A33%3A38Z&sp=rwdl&sig=dummysignature")]
    [InlineData("sv=2025-07-05&ss=b&srt=sc&spr=https&st=2000-01-01T07%3A33%3A38Z&se=2099-12-31T07%3A33%3A38Z&sp=lacp&sig=dummysignature")]
    public async Task VerifyConnection_WhenInvalidSasTokenPermissions_ReturnInvalidSasTokenPermissionsErrorMessage(string sasToken)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AzureDataLakeConfigurationConstants.AccountKey)] = sasToken;
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.Equal(AzureDataLakeConnector.InvalidSasTokenPermissionsErrorMessage, result.ErrorMessage);

        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    [InlineData("ALLCAPS")]
    [InlineData("someCaps")]
    [InlineData("some space")]
    [InlineData("1 2")]
    [InlineData("invalid_file_system")]
    [InlineData("ab")] // too short
    [InlineData("abc012345678901234567890123456789012345678901234567890123456789z")] // too long
    [InlineData("-invalidstart")]
    [InlineData("invalidend-")]
    public async Task VerifyConnection_WhenInvalidFileSystemName_ReturnInvalidFileSystemNameErrorMessage(string fileSystemName)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AzureDataLakeConfigurationConstants.FileSystemName)] = fileSystemName;
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.Equal(AzureDataLakeConnector.InvalidFileSystemNameErrorMessage, result.ErrorMessage);
        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    [InlineData("File/one/two.")]
    [InlineData("File/one//two")]
    [InlineData("File./one/two")]
    public async Task VerifyConnection_WhenInvalidDirectoryName_ReturnInvalidFileSystemNameErrorMessage(string directoryName)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AzureDataLakeConfigurationConstants.DirectoryName)] = directoryName;
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.Equal(AzureDataLakeConnector.InvalidDirectoryNameErrorMessage, result.ErrorMessage);
        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    [Theory]
    [InlineData("Files")]
    [InlineData("Files/one")]
    [InlineData("Files/one/two")]
    public async Task VerifyConnection_WhenValidDirectoryName_ReturnSuccess(string directoryName)
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        configuration[nameof(AzureDataLakeConfigurationConstants.DirectoryName)] = directoryName;
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        try
        {
            var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
            var connector = setupResult.ConnectorMock.Object;
            setupResult.StorageFactoryMock.Setup(factory => factory.CreateStorageConfiguration(setupResult.Context, configuration, It.IsAny<string>()))
                .Returns(Task.FromResult<IStorageConfiguration>(jobData));
            var result = await connector.VerifyConnection(setupResult.Context, configuration);
            Assert.NotNull(result);
            Assert.True(result.Success);
        }
        catch
        {
            DeleteFileSystemIfExists(jobData);
            throw;
        }
    }

    [Fact]
    public async Task VerifyStoreData_EventStream()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            setupResult,
            assertMethod: async (setupResult, exportedFilePath) =>
            {
                await AssertJsonResult(setupResult, exportedFilePath, StreamMode.EventStream, VersionChangeType.Added);
            });
    }

    [Fact]
    public async Task VerifyStoreData_EventStream_CanUseAccountKey()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            setupResult,
            assertMethod: async (setupResult, exportedFilePath) =>
            {
                await AssertJsonResult(setupResult, exportedFilePath, StreamMode.EventStream, VersionChangeType.Added);
            });
    }

    [Fact]
    public async Task VerifyStoreData_EventStream_CanUseServicePrincipalAndWithoutKeyVault()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        UpdateConfigurationWithServicePrincipalAuthentication(configuration);
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            setupResult,
            assertMethod: async (setupResult, exportedFilePath) =>
            {
                await AssertJsonResult(setupResult, exportedFilePath, StreamMode.EventStream, VersionChangeType.Added);
            });
    }

    [Fact]
    public async Task VerifyStoreData_EventStream_CanUseServicePrincipalAndWithKeyVault()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        UpdateConfigurationWithServicePrincipalAuthentication(configuration);
        UpdateConfigurationWithKeyVault(configuration);
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            setupResult,
            assertMethod: async (setupResult, exportedFilePath) =>
            {
                await AssertJsonResult(setupResult, exportedFilePath, StreamMode.EventStream, VersionChangeType.Added);
            });
    }

    [Fact(Skip = "Workload identity tests are currently skipped until we setup federation using oidc https://learn.microsoft.com/en-us/graph/api/federatedidentitycredential-post?view=graph-rest-1.0&tabs=http")]
    public async Task VerifyStoreData_EventStream_CanUseWorkloadIdentityAndWithoutKeyVault()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        UpdateConfigurationWithWorkloadIdentityConfiguration(configuration);
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            setupResult,
            assertMethod: async (setupResult, exportedFilePath) =>
            {
                await AssertJsonResult(setupResult, exportedFilePath, StreamMode.EventStream, VersionChangeType.Added);
            });
    }

    [Fact(Skip = "Workload identity tests are currently skipped until we setup federation using oidc https://learn.microsoft.com/en-us/graph/api/federatedidentitycredential-post?view=graph-rest-1.0&tabs=http")]
    public async Task VerifyStoreData_EventStream_CanUseWorkloadIdentityAndWithKeyVault()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        UpdateConfigurationWithWorkloadIdentityConfiguration(configuration);
        UpdateConfigurationWithKeyVault(configuration);
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.EventStream);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.EventStream, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            setupResult,
            assertMethod: async (setupResult, exportedFilePath) =>
            {
                await AssertJsonResult(setupResult, exportedFilePath, StreamMode.EventStream, VersionChangeType.Added);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithoutStreamCache()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;

        var data = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added);
        await connector.StoreData(setupResult.Context, setupResult.StreamModel, data);
        await AssertImmediateOutputResult(
            setupResult,
            assertMethod: async (setupResult, filePath) =>
            {
                await AssertJsonResult(setupResult, filePath, StreamMode.Sync, VersionChangeType.Added, isSingleObject: true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndJsonFormat()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "JSON",
            assertMethod: async (setupResult, filePath) =>
            {
                await AssertJsonResult(setupResult, filePath, StreamMode.Sync, VersionChangeType.Added);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndCsvFormatUnescaped()
    {
        await VerifyStoreData_Sync_WithStreamCache("csv", AssertCsvResultUnescaped);
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndCsvFormatEscaped()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultEscaped,
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatUnescaped()
    {
        await VerifyStoreData_Sync_WithStreamCache("pArQuet", AssertParquetResultUnescaped);
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatWithEscaped()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "pArQuet",
            AssertParquetResultEscaped,
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormatWithArrayColumnsEnabled()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "pArQuet",
            AssertParquetResultArrayColumnEnabled,
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.IsArrayColumnsEnabled), true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndSameDataTime_CanSkip()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
            async executeExportArg =>
            {
                var jobArgs = new StorageJobArgs
                {
                    OrganizationId = executeExportArg.Organization.Id.ToString(),
                    Schedule = "0 0/1 * * *",
                    Message = executeExportArg.StreamId.ToString(),
                    IsTriggeredFromJobServer = false,
                };
                _ = await executeExportArg.ExportJob.DoRunInternalAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var firstPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                var secondResult = await executeExportArg.ExportJob.DoRunInternalAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);
                var secondDataTime = await GetFileDataTime(executeExportArg, secondPath);

                Assert.Equal(firstDataTime, secondDataTime);
                Assert.Equal(StorageExportEntitiesJobBase.ExportedBeforeReason, secondResult.Reason);
                return secondPath;
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingInternalSchedulerAndDifferentDataTime_CanOverwrite()
    {
        var executionCount = 0;
        var dateTimeList = new List<DateTimeOffset>
        {
            DefaultCurrentTime,
            new DateTimeOffset(2024, 8, 21, 4, 16, 0, TimeSpan.FromHours(5)),
        };
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
            async executeExportArg =>
            {
                var jobArgs = new StorageJobArgs
                {
                    OrganizationId = executeExportArg.Organization.Id.ToString(),
                    Schedule = "0 0 1-31 * *",
                    Message = executeExportArg.StreamId.ToString(),
                    IsTriggeredFromJobServer = false,
                };
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                executionCount++;

                var firstPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(
                    executeExportArg.SetupContainerResult,
                    filterPaths: paths =>
                    {
                        return paths.Where(path => path.Name != firstPath.Name).ToList();
                    });
                var secondDataTime = await GetFileDataTime(executeExportArg, secondPath);

                Assert.NotEqual(firstDataTime, secondDataTime);
                return secondPath;
            },
            mockDateTimeOffsetProvider =>
            {
                mockDateTimeOffsetProvider.Setup(x => x.GetCurrentUtcTime())
                    .Returns(() =>
                    {
                        return dateTimeList[executionCount].ToUniversalTime();
                    });
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WhenRepeatRunAndFileExistsUsingJobServer_CanCreateNewFile()
    {
        var executionCount = 0;
        var dateTimeList = new List<DateTimeOffset>
        {
            DefaultCurrentTime,
            new DateTimeOffset(2024, 8, 21, 4, 16, 0, TimeSpan.FromHours(5)),
        };
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
            async executeExportArg =>
            {
                var jobArgs = new StorageJobArgs
                {
                    OrganizationId = executeExportArg.Organization.Id.ToString(),
                    Schedule = "0 0/1 * * *",
                    Message = executeExportArg.StreamId.ToString(),
                    IsTriggeredFromJobServer = false,
                };
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);
                executionCount++;

                var firstPath = await WaitForFileToBeCreated(executeExportArg.SetupContainerResult);

                var firstDataTime = await GetFileDataTime(executeExportArg, firstPath);
                await executeExportArg.ExportJob.DoRunAsync(
                    executeExportArg.ExecutionContext,
                    jobArgs);

                var secondPath = await WaitForFileToBeCreated(
                    executeExportArg.SetupContainerResult,
                    filterPaths: paths =>
                    {
                        return paths.Where(path => path.Name != firstPath.Name).ToList();
                    });
                var secondDataTime = await GetFileDataTime(executeExportArg, secondPath);

                Assert.NotEqual(firstDataTime, secondDataTime);
                return secondPath;
            },
            mockDateTimeOffsetProvider =>
            {
                mockDateTimeOffsetProvider.Setup(x => x.GetCurrentUtcTime())
                    .Returns(() =>
                    {
                        return dateTimeList[executionCount].ToUniversalTime();
                    });
            });
    }

    [Fact]
    public async Task GetContainers_InvalidParamsTest()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        var containers = await connector.GetContainers(setupResult.Context, setupResult.ProviderDefinition.Id);
        Assert.Empty(containers);

        //This is an existing container in the Azure Data Lake account
        //There are existing files in the directory
        //Changing this or removing the files will cause the test to fail
        configuration[AzureDataLakeConfigurationConstants.FileSystemName] = "apac-container";
        configuration[AzureDataLakeConfigurationConstants.DirectoryName] = "TestExport01";

        containers = await connector.GetContainers(setupResult.Context, setupResult.ProviderDefinition.Id);
        Assert.NotEmpty(containers);
    }

    [Fact]
    public async Task GetContainers_HasValuesTest()
    {
        var configuration = CreateConfigurationWithoutStreamCache();
        //This is an existing container in the Azure Data Lake account
        //There are existing files in the directory
        //Changing this or removing the files will cause the test to fail
        configuration[AzureDataLakeConfigurationConstants.FileSystemName] = "apac-container";
        configuration[AzureDataLakeConfigurationConstants.DirectoryName] = "TestExport01";
        var jobData = new AzureDataLakeConnectorConfiguration(configuration);

        var setupResult = await SetupContainer(jobData, StreamMode.Sync);
        var connector = setupResult.ConnectorMock.Object;
        var containers = await connector.GetContainers(setupResult.Context, setupResult.ProviderDefinition.Id);

        Assert.NotNull(containers);
        Assert.NotEmpty(containers);
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheCanIgnoreWhenChangedAfterDeletion()
    {
        await VerifyStoreData_Sync_WithStreamCache("csv",
            async (setupResult, filePath) => await AssertCsvResult(setupResult, filePath, "_", (rows) =>
            {
                var removed = rows.ToList();
                removed.Clear();
                return removed;
            }),
            getConnectorEntityData: () =>
            {
                var initialUserData = UserData.Default;
                var removedUserData = initialUserData with { Age = initialUserData.Age + 1 };
                var readdAfterDeletionUserData = initialUserData with { Age = initialUserData.Age + 2 };

                var initialEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Added, persistVersion: 1, userData: initialUserData);
                var removedEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Removed, persistVersion: 2, userData: removedUserData);
                var readdAfterDeletionEntityData = CreateBaseConnectorEntityData(StreamMode.Sync, VersionChangeType.Changed, persistVersion: 3, userData: readdAfterDeletionUserData);

                // Intermediate version is outdated when final version is stored, so it should be ignored and not cause the export to fail
                return new[] { initialEntityData, removedEntityData, readdAfterDeletionEntityData };
            },
            configureAuthentication: (values) =>
            {
                values.Add(nameof(StorageConfigurationConstants.ShouldEscapeVocabularyKeys), true);
                values.Add(nameof(StorageConfigurationConstants.ShouldWriteGuidAsString), true);
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheUsingSasToken()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
            configureAuthentication: (values) =>
            {
                var sasToken = SasTokenHelper.CreateSasToken(
                    values[nameof(AzureDataLakeConfigurationConstants.AccountName)] as string,
                    values[nameof(AzureDataLakeConfigurationConstants.AccountKey)] as string);
                values[nameof(AzureDataLakeConfigurationConstants.AccountKey)] = sasToken;
            });
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheUsingServicePrincipalWithoutKeyVault()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
            configureAuthentication: UpdateConfigurationWithServicePrincipalAuthentication);
    }

    [Fact]
    public async Task VerifyStoreData_Sync_WithStreamCacheUsingServicePrincipalWithKeyVault()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
            configureAuthentication: (values) =>
            {
                UpdateConfigurationWithServicePrincipalAuthentication(values);
                UpdateConfigurationWithKeyVault(values);
            });
    }

    [Fact(Skip = "Workload identity tests are currently skipped until we setup federation using oidc https://learn.microsoft.com/en-us/graph/api/federatedidentitycredential-post?view=graph-rest-1.0&tabs=http")]
    public async Task VerifyStoreData_Sync_WithStreamCacheUsingWorkloadIdentityWithoutKeyVault()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
            configureAuthentication: UpdateConfigurationWithWorkloadIdentityConfiguration);
    }

    [Fact(Skip = "Workload identity tests are currently skipped until we setup federation using oidc https://learn.microsoft.com/en-us/graph/api/federatedidentitycredential-post?view=graph-rest-1.0&tabs=http")]
    public async Task VerifyStoreData_Sync_WithStreamCacheUsingWorkloadIdentityWithKeyVault()
    {
        await VerifyStoreData_Sync_WithStreamCache(
            "csv",
            AssertCsvResultUnescaped,
            configureAuthentication: (values) =>
            {
                UpdateConfigurationWithWorkloadIdentityConfiguration(values);
                UpdateConfigurationWithKeyVault(values);
            });
    }

    private protected override StorageExportEntitiesJobBase CreateExportJob(SetupContainerResult setupResult)
    {
        var exportJob = new AzureDataLakeExportEntitiesJob(
            setupResult.ApplicationContext,
            setupResult.StreamRepositoryMock.Object,
            setupResult.ConstantsMock.Object,
            setupResult.StorageFactoryMock.Object,
            setupResult.DateTimeOffsetProviderMock.Object);
        return exportJob;
    }

    private protected override DataLakeServiceClient GetDataLakeClient(SetupContainerResult setupContainerResult)
    {
        var jobData = setupContainerResult.StorageConfiguration as AzureDataLakeConnectorConfiguration;
        return GetDataLakeClient(jobData);
    }

    private DataLakeServiceClient GetDataLakeClient(AzureDataLakeConnectorConfiguration jobData)
    {
        var configuration = CreateConfigurationWithoutStreamCache();

        return new DataLakeServiceClient(
            new Uri($"https://{jobData.AccountName}.dfs.core.windows.net"),
        new StorageSharedKeyCredential(
            configuration[nameof(AzureDataLakeConfigurationConstants.AccountName)] as string,
            configuration[nameof(AzureDataLakeConfigurationConstants.AccountKey)] as string));
    }

    private protected override StorageConfigurationBase CreateStorageConfiguration(Dictionary<string, object> configuration)
    {
        return new AzureDataLakeConnectorConfiguration(configuration);
    }

    private protected override string GetDirectoryName(SetupContainerResult setupContainerResult)
    {
        var jobData = setupContainerResult.StorageConfiguration as AzureDataLakeConnectorConfiguration;
        return jobData.DirectoryName;
    }

    private protected override Dictionary<string, object> CreateConfigurationWithoutStreamCache()
    {
        var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
        Assert.NotNull(accountName);
        var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
        Assert.NotNull(accountKey);

        var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
        var directoryName = $"xunit-{DateTime.Now.Ticks}";
        return new Dictionary<string, object>()
        {
            { nameof(AzureDataLakeConfigurationConstants.AccountName), accountName },
            { nameof(AzureDataLakeConfigurationConstants.AccountKey), accountKey },
            { nameof(AzureDataLakeConfigurationConstants.FileSystemName), fileSystemName },
            { nameof(AzureDataLakeConfigurationConstants.DirectoryName), directoryName },
        };
    }
    private static void UpdateConfigurationWithServicePrincipalAuthentication(Dictionary<string, object> configuration)
    {
        configuration[nameof(AzureDataLakeConfigurationConstants.AuthenticationMethod)] = AuthenticationMethods.ServicePrincipal.ToString();
        configuration[nameof(AzureDataLakeConfigurationConstants.TenantId)] = Environment.GetEnvironmentVariable("ADL2_TENANTID");
        configuration[nameof(AzureDataLakeConfigurationConstants.ClientId)] = Environment.GetEnvironmentVariable("ADL2_CLIENTID");
        configuration[nameof(AzureDataLakeConfigurationConstants.ClientSecret)] = Environment.GetEnvironmentVariable("ADL2_CLIENTSECRET");
    }

    private static void UpdateConfigurationWithWorkloadIdentityConfiguration(Dictionary<string, object> configuration)
    {
        configuration[nameof(AzureDataLakeConfigurationConstants.AuthenticationMethod)] = AuthenticationMethods.WorkloadIdentity.ToString();
    }

    private static void UpdateConfigurationWithKeyVault(Dictionary<string, object> configuration)
    {
        configuration[nameof(AzureDataLakeConfigurationConstants.UseKeyVault)] = true;
        configuration[nameof(AzureDataLakeConfigurationConstants.KeyVaultUri)] = Environment.GetEnvironmentVariable("ADL2_KEYVAULTURI");
        configuration[nameof(AzureDataLakeConfigurationConstants.KeyVaultSecretName)] = Environment.GetEnvironmentVariable("ADL2_KEYVAULTSECRETNAME");
    }

    protected override Mock<IAzureDataLakeConfigurationConstants> CreateConstantsMock()
    {
        var constants = base.CreateConstantsMock();
        constants.Setup(x => x.WorkloadIdentityAuthenticationMethodEnabledKeyName).Returns("abc");
        constants.Setup(x => x.WorkloadIdentityAuthenticationMethodEnabledDefaultValue).Returns(false);
        return constants;
    }

    protected override Mock<AzureDataLakeConnector> GetConnectorMock(
        ApplicationContext applicationContext,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<IAzureDataLakeConfigurationConstants> constantsMock,
        Mock<AzureDataLakeStorageFactory> jobDataFactory)
    {
        var mockConnector = new Mock<AzureDataLakeConnector>(
            new Mock<ILogger<AzureDataLakeConnector>>().Object,
            applicationContext,
            constantsMock.Object,
            jobDataFactory.Object,
            mockDateTimeOffsetProvider.Object);
        return mockConnector;
    }

    protected override Mock<AzureDataLakeStorageFactory> CreateStorageFactoryMock(
        WindsorContainer container,
        ApplicationContext applicationContext,
        Mock<IDateTimeOffsetProvider> mockDateTimeOffsetProvider,
        Mock<IAzureDataLakeConfigurationConstants> constantsMock)
    {
        var dataFactoryMock = new Mock<AzureDataLakeStorageFactory>(constantsMock.Object);
        dataFactoryMock.Setup(x => x.CreateStorageClient(It.IsAny<ExecutionContext>(), It.IsAny<IStorageConfiguration>()))
            .Returns<ExecutionContext, IStorageConfiguration>(
                (_, data) => Task.FromResult<IStorageClient>(
                    new AzureDataLakeStorageClient(
                        NullLogger<AzureDataLakeStorageClient>.Instance,
                        data as AzureDataLakeConnectorConfiguration,
                        constantsMock.Object)));
        return dataFactoryMock;
    }
}

