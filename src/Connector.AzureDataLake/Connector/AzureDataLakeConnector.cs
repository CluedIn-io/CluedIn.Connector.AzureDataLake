using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using CluedIn.Connector.Common.Connectors;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeConnector : CommonConnectorBase<AzureDataLakeConnector, IAzureDataLakeClient>
    {
        protected readonly Dictionary<string, List<object>> BulkCache = new Dictionary<string, List<object>>();

        public readonly int Threshold = 1000;

        public AzureDataLakeConnector(IConfigurationRepository repository, ILogger<AzureDataLakeConnector> logger,
            IAzureDataLakeClient client, IAzureDataLakeConstants constants)
            : base(repository, logger, client, constants.ProviderId)
        {
            // TODO: ROK:
        }

        public override Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            CreateContainerModel model)
        {
            return Task.CompletedTask;
        }


        public override Task<string> GetValidContainerName(ExecutionContext executionContext,
            Guid providerDefinitionId, string name)
        {
            // Strip non-alpha numeric characters
            return Uri.TryCreate(name, UriKind.Absolute, out var uri)
                ? Task.FromResult(uri.AbsolutePath)
                : Task.FromResult(name);
        }

        public override Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext,
            Guid providerDefinitionId)
        {
            return Task.FromResult<IEnumerable<IConnectorContainer>>(new List<IConnectorContainer>());
        }

        public override Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext,
            Guid providerDefinitionId, string containerId)
        {
            return Task.FromResult<IEnumerable<IConnectionDataType>>(new List<IConnectionDataType>());
        }

        public override async Task StoreData(ExecutionContext executionContext, Guid providerDefinitionId,
            string containerName, IDictionary<string, object> data)
        {
            data["ProviderDefinitionId"] = providerDefinitionId;
            data["ContainerName"] = containerName;

            var bulk = AddDataToCache(providerDefinitionId, containerName, data);

            if (bulk.Count >= Threshold)
                await Flush(executionContext, providerDefinitionId, containerName, bulk);

            executionContext.Log.LogDebug(
                $"AzureDataLakeConnector.StoreData:\n{JsonUtility.SerializeIndented(data)}\n");
        }

        private List<object> AddDataToCache(Guid providerDefinitionId, string containerName,
            IDictionary<string, object> data, bool isEdges = false)
        {
            var bulkCacheKeyName = $"{providerDefinitionId}-{containerName}";
            if (isEdges)
                bulkCacheKeyName += "-edges";

            if (!BulkCache.TryGetValue(bulkCacheKeyName, out var bulk))
            {
                bulk = new List<object>();
                BulkCache[bulkCacheKeyName] = bulk;
            }

            bulk.Add(data);

            return bulk;
        }

        public static DataLakeServiceClient GetDataLakeServiceClient(AzureDataLakeConnectorJobData jobData)
        {
            return new DataLakeServiceClient(
                new Uri($"https://{jobData.AccountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(jobData.AccountName, jobData.AccountKey));
        }

        public static async Task<DataLakeFileSystemClient> EnsureDataLakeFileSystemClientAsync(
            AzureDataLakeConnectorJobData jobData)
        {
            var dataLakeServiceClient = GetDataLakeServiceClient(jobData);

            var dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(jobData.FileSystemName);

            if (!await dataLakeFileSystemClient.ExistsAsync())
                dataLakeFileSystemClient = await dataLakeServiceClient.CreateFileSystemAsync(jobData.FileSystemName);

            return dataLakeFileSystemClient;
        }

        public static async Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryClientAsync(
            AzureDataLakeConnectorJobData jobData)
        {
            var dataLakeFileSystemClient = await EnsureDataLakeFileSystemClientAsync(jobData);

            var directoryClient = dataLakeFileSystemClient.GetDirectoryClient(jobData.DirectoryName);

            if (!await directoryClient.ExistsAsync())
                directoryClient = await dataLakeFileSystemClient.CreateDirectoryAsync(jobData.DirectoryName);

            return directoryClient;
        }


        public async Task Flush(ExecutionContext executionContext, Guid providerDefinitionId, string containerName,
            List<object> bulk)
        {
            executionContext.Log.LogDebug(
                $"AzureDataLakeConnector.Flush: providerDefinitionId: {providerDefinitionId}, containerName: {containerName}, bulkSize: {bulk.Count}");

            // TODO: This must have a better concurrency handling
            var copy = new List<object>(bulk);
            bulk.Clear();

            var connection = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var jobData = new AzureDataLakeConnectorJobData(connection.Authentication);
            var directoryClient = await EnsureDataLakeDirectoryClientAsync(jobData);

            var content = JsonUtility.SerializeIndented(copy);
            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm-ss");
            var fileName = $"{containerName}.{timestamp}.json";

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);

            await using var memoryStream = new MemoryStream();
            await using var streamWriter = new StreamWriter(memoryStream);
            await streamWriter.WriteAsync(content);
            memoryStream.Position = 0;

            var options = new DataLakeFileUploadOptions
            {
                HttpHeaders = new PathHttpHeaders { ContentType = "application/json" }
            };

            await dataLakeFileClient.UploadAsync(memoryStream, options);


            executionContext.Log.LogDebug(
                $"AzureDataLakeConnector.Flush: providerDefinitionId: {providerDefinitionId}, containerName: {containerName}\n{JsonUtility.SerializeIndented(bulk)}\n");
        }

        public override async Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId,
            string containerName, string originEntityCode, IEnumerable<string> edges)
        {
            var data = new Dictionary<string, object>
            {
                {"ProviderDefinitionId", providerDefinitionId.ToString()},
                {"ContainerName", containerName},
                {"OriginEntityCode", originEntityCode},
                {"Edges", edges}
            };

            var bulk = AddDataToCache(providerDefinitionId, containerName, data, true);

            if (bulk.Count >= Threshold)
                await Flush(executionContext, providerDefinitionId, $"{containerName}.edges", bulk);

            executionContext.Log.LogDebug(
                $"AzureDataLakeConnector.StoreEdgeData:\n{JsonUtility.SerializeIndented(data)}\n");
        }

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext,
            IDictionary<string, object> authenticationData)
        {
            await EnsureDataLakeDirectoryClientAsync(new AzureDataLakeConnectorJobData(authenticationData));
            return true;
        }

        public override Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            throw new NotImplementedException(nameof(EmptyContainer));
        }

        public override Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            throw new NotImplementedException(nameof(ArchiveContainer));
        }

        public override Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id, string newName)
        {
            throw new NotImplementedException(nameof(RenameContainer));
        }

        public override Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            throw new NotImplementedException(nameof(RemoveContainer));
        }
    }
}
