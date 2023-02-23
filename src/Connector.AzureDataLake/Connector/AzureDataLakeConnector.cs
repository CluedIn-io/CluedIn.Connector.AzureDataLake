using CluedIn.Connector.Common.Caching;
using CluedIn.Connector.Common.Connectors;
using CluedIn.Core;
using CluedIn.Core.Configuration;
using CluedIn.Core.Connectors;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeConnector : CommonConnectorBase<AzureDataLakeConnector, IAzureDataLakeClient>
    {
        private readonly InMemoryCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData> _cachingService;
        private readonly object _cacheLock;
        private readonly int _cacheRecordsThreshold;

        private DateTime _lastStoreDataAt;

        private readonly CancellationTokenSource _backgroundFlushingCancellationTokenSource;

        public AzureDataLakeConnector(IConfigurationRepository repository,
            ILogger<AzureDataLakeConnector> logger,
            IAzureDataLakeClient client,
            IAzureDataLakeConstants constants,
            InMemoryCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData> cachingService)
            : base(repository, logger, client, constants.ProviderId)
        {
            _cachingService = cachingService;
            _cacheLock = _cachingService.Locker;
            _cacheRecordsThreshold = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheRecordsThresholdKeyName, constants.CacheRecordsThresholdDefaultValue);
            var backgroundFlushMaxIdleDefaultValue = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheSyncIntervalKeyName, constants.CacheSyncIntervalDefaultValue);

            _backgroundFlushingCancellationTokenSource = new CancellationTokenSource();

            Task.Run(() =>
            {
                while (true)
                {
                    _backgroundFlushingCancellationTokenSource.Token.WaitHandle.WaitOne(1000);
                    lock (_cacheLock)
                    {

                        if (_backgroundFlushingCancellationTokenSource.IsCancellationRequested)
                        {
                            return;
                        }

                        if (DateTime.Now.Subtract(_lastStoreDataAt).TotalMilliseconds > backgroundFlushMaxIdleDefaultValue)
                        {
                            Flush();
                        }
                    }
                }
            }, _backgroundFlushingCancellationTokenSource.Token);
        }

        ~AzureDataLakeConnector()
        {
            _backgroundFlushingCancellationTokenSource.Cancel();

            Flush();
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
                _lastStoreDataAt = DateTime.Now;
                _cachingService.AddItem(data, configurations).GetAwaiter().GetResult();
                var count = _cachingService.Count().GetAwaiter().GetResult();

                if (count >= _cacheRecordsThreshold)
                {
                    Flush();
                }
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
                var count = _cachingService.Count().GetAwaiter().GetResult();

                if (count >= _cacheRecordsThreshold)
                {
                    Flush();
                }
            }
        }

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext,
            IDictionary<string, object> authenticationData)
        {
            await _client.EnsureDataLakeDirectoryExist(new AzureDataLakeConnectorJobData(authenticationData));

            return true;
        }

        private void Flush()
        {
            lock (_cacheLock)
            {
                var itemsCount = _cachingService.Count().GetAwaiter().GetResult();
                if (itemsCount == 0)
                {
                    return;
                }

                var cachedItems = _cachingService.GetItems().GetAwaiter().GetResult();
                var cachedItemsByConfigurations = cachedItems.GroupBy(pair => pair.Value).ToList();

                var settings = new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.None,
                    Formatting = Formatting.Indented,
                };

                foreach (var group in cachedItemsByConfigurations)
                {
                    var configuration = group.Key;
                    var content = JsonConvert.SerializeObject(group.Select(g => g.Key), settings);

                    var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm-ss");
                    var fileName = $"{configuration.ContainerName}.{timestamp}.json";

                    ActionExtensions.ExecuteWithRetry(() =>
                    {
                        _client.SaveData(configuration, content, fileName).GetAwaiter().GetResult();
                    });


                    _cachingService.Clear(configuration).GetAwaiter().GetResult();
                }
            }
        }

        public override Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            CreateContainerModel model)
        {
            return Task.CompletedTask;
        }

        public override Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            lock (_cacheLock)
            {
                try
                {
                    Flush();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"{nameof(AzureDataLakeConnector)} fails to save entities before reprocessing");
                    _cachingService.Clear().GetAwaiter().GetResult();
                }
            }

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
    }
}
