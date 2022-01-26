﻿using CluedIn.Connector.Common.Caching;
using CluedIn.Connector.Common.Connectors;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeConnector : CommonConnectorBase<AzureDataLakeConnector, IAzureDataLakeClient>, IScheduledSyncs
    {
        public readonly int Threshold = 50;
        private readonly ICachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData> _cachingService;
        private readonly object _cacheLock = new object();

        public AzureDataLakeConnector(IConfigurationRepository repository,
            ILogger<AzureDataLakeConnector> logger,
            IAzureDataLakeClient client,
            IAzureDataLakeConstants constants,
            ICachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData> batchingService)
            : base(repository, logger, client, constants.ProviderId)
        {
            _cachingService = batchingService;
        }

        public override async Task StoreData(ExecutionContext executionContext, Guid providerDefinitionId,
            string containerName, IDictionary<string, object> data)
        {
            data["ProviderDefinitionId"] = providerDefinitionId;
            data["ContainerName"] = containerName;

            var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            var configurations = new AzureDataLakeConnectorJobData(connection.Authentication, containerName);

            lock (_cacheLock)
            {
                _cachingService.AddItem(data, configurations).GetAwaiter().GetResult();
            }

            if (await _cachingService.Count() >= Threshold)
            {
                Flush();
            }
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

            var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            var configurations = new AzureDataLakeConnectorJobData(connection.Authentication, $"{containerName}.edges");

            lock (_cacheLock)
            {   
                _cachingService.AddItem(data, configurations).GetAwaiter().GetResult();             
            }

            if (await _cachingService.Count() >= Threshold)
            {
                Flush();
            }            
        }

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext,
            IDictionary<string, object> authenticationData)
        {
            _logger.LogInformation($"AzureDataLakeConnector.VerifyConnection: entry");
            
            ValidateAuthenticationData(authenticationData);

            await _client.EnsureDataLakeDirectoryExist(new AzureDataLakeConnectorJobData(authenticationData));

            _logger.LogInformation($"AzureDataLakeConnector.VerifyConnection: exit");

            return true;
        }

        public async Task Sync()
        {
            _logger.LogInformation($"AzureDataLakeConnector.Sync: entry");

            if (await _cachingService.Count() == 0)
                return;

            Flush();
        }

        private void Flush()
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;
            _logger.LogInformation($"AzureDataLakeConnector.Flush: entry {threadId}");
            lock (_cacheLock)
            {
                var itemsCount = _cachingService.Count().GetAwaiter().GetResult();
                _logger.LogInformation($"AzureDataLakeConnector.Flush: IC-{itemsCount} lock aquired {threadId}");
                if (itemsCount == 0)
                    return;

                var cachedItems = _cachingService.GetItems().GetAwaiter().GetResult();
                var cachedItemsByConfigurations = cachedItems.GroupBy(pair => pair.Value)
                    .ToList();
                _logger.LogInformation($"AzureDataLakeConnector.Flush group count {cachedItemsByConfigurations.Count}");

                foreach (var group in cachedItemsByConfigurations)
                {
                    var configuration = group.Key;
                    var content = JsonUtility.SerializeIndented(group.Select(g => g.Key));

                    _client.SaveData(configuration, content).GetAwaiter().GetResult();
                    _cachingService.Clear(configuration).GetAwaiter().GetResult();
                }

                itemsCount = _cachingService.Count().GetAwaiter().GetResult();
                _logger.LogInformation($"AzureDataLakeConnector.Flush: IC-{itemsCount} lock almost released {threadId}");
            }

            _logger.LogInformation($"AzureDataLakeConnector.Flush: exit {threadId}");
        }

        public override Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            CreateContainerModel model)
        {
            return Task.CompletedTask;
        }

        public override Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            _logger.LogInformation($"AzureDataLakeConnector.ArchiveContainer: entry");

            return Task.CompletedTask;
        }

        #region Not supported overrides

        public override Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext,
            Guid providerDefinitionId)
        {
            _logger.LogInformation($"AzureDataLakeConnector.GetContainers: entry");

            throw new NotImplementedException(nameof(GetContainers));
        }

        public override Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext,
            Guid providerDefinitionId, string containerId)
        {
            _logger.LogInformation($"AzureDataLakeConnector.GetDataTypes: entry");

            throw new NotImplementedException(nameof(GetDataTypes));
        }

        public override Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            _logger.LogInformation($"AzureDataLakeConnector.EmptyContainer: entry");

            throw new NotImplementedException(nameof(EmptyContainer));
        }

        public override Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id, string newName)
        {
            _logger.LogInformation($"AzureDataLakeConnector.RenameContainer: entry");

            throw new NotImplementedException(nameof(RenameContainer));
        }

        public override Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            _logger.LogInformation($"AzureDataLakeConnector.RemoveContainer: entry");

            throw new NotImplementedException(nameof(RemoveContainer));
        }

        #endregion

        private void ValidateAuthenticationData(IDictionary<string, object> authenticationData)
        {
            if (authenticationData == null)
            {
                throw new ArgumentNullException("Authentication Data is empty!");
            }

            ValidateFileSystemName(authenticationData[AzureDataLakeConstants.FileSystemName].ToString());
        }

        public void ValidateFileSystemName(string fileSystemName)
        {
            //FileSystem Length must be 3 to 63
            if (!Enumerable.Range(3, 63).Contains(fileSystemName.Length))
            {
                throw new ArgumentException("File System Name did not meet the required length.");
            }

            var regEx = new Regex("^[a-z0-9]+(-[a-z0-9]+)*$");
            if (!regEx.IsMatch(fileSystemName))
            {
                throw new ArgumentException("Invalid File System Name");
            }
        }
    }
}
