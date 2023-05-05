using CluedIn.Core.Configuration;
using CluedIn.Core.Connectors;
using CluedIn.Core.DataStore;
using CluedIn.Core.Processing;
using CluedIn.Core.Streams.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeConnector : ConnectorBaseV2
    {
        private readonly ILogger<AzureDataLakeConnector> _logger;
        private readonly IAzureDataLakeClient _client;

        //private DateTime _lastStoreDataAt;

        private readonly PartitionedBuffer<(IReadOnlyConnectorEntityData, AzureDataLakeConnectorJobData)> _buffer;

        //private readonly CancellationTokenSource _backgroundFlushingCancellationTokenSource;

        public AzureDataLakeConnector(
            ILogger<AzureDataLakeConnector> logger,
            IAzureDataLakeClient client,
            IAzureDataLakeConstants constants)
            : base(constants.ProviderId, false)
        {
            _logger = logger;
            _client = client;


            var cacheRecordsThreshold = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheRecordsThresholdKeyName, constants.CacheRecordsThresholdDefaultValue);
            var backgroundFlushMaxIdleDefaultValue = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheSyncIntervalKeyName, constants.CacheSyncIntervalDefaultValue);

            _buffer = new PartitionedBuffer<(IReadOnlyConnectorEntityData, AzureDataLakeConnectorJobData)>(cacheRecordsThreshold,
                backgroundFlushMaxIdleDefaultValue, Flush);

            //_backgroundFlushingCancellationTokenSource = new CancellationTokenSource();

            //Task.Run(() =>
            //{
            //    while (true)
            //    {
            //        lock (_cacheLock)
            //        {
            //            _backgroundFlushingCancellationTokenSource.Token.WaitHandle.WaitOne(1000);

            //            if (_backgroundFlushingCancellationTokenSource.IsCancellationRequested)
            //            {
            //                return;
            //            }

            //            if (DateTime.Now.Subtract(_lastStoreDataAt).TotalMilliseconds > backgroundFlushMaxIdleDefaultValue)
            //            {
            //                Flush();
            //            }
            //        }
            //    }
            //}, _backgroundFlushingCancellationTokenSource.Token);
        }

        ~AzureDataLakeConnector()
        {
            _buffer.Dispose();
            //_backgroundFlushingCancellationTokenSource.Cancel();

            //Flush();
        }

        public override Task VerifyExistingContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            return Task.FromResult(0);
        }

        public override async Task<SaveResult> StoreData(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, IReadOnlyConnectorEntityData connectorEntityData)
        {
            var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
            var containerName = streamModel.ContainerName;

            var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            var configurations = new AzureDataLakeConnectorJobData(connection.Authentication.ToDictionary(x => x.Key, x => x.Value), containerName);

            await _buffer.Add((connectorEntityData, configurations), JsonConvert.SerializeObject(configurations));

            return SaveResult.Success;

            //data["ProviderDefinitionId"] = providerDefinitionId;
            //data["ContainerName"] = containerName;

            //var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            //var configurations = new AzureDataLakeConnectorJobData(connection.Authentication, containerName);

            //lock (_cacheLock)
            //{
            //    _lastStoreDataAt = DateTime.Now;
            //    _cachingService.AddItem(data, configurations).GetAwaiter().GetResult();
            //    var count = _cachingService.Count().GetAwaiter().GetResult();

            //    if (count >= _cacheRecordsThreshold)
            //    {
            //        Flush();
            //    }
            //}
        }

        public override Task<ConnectorLatestEntityPersistInfo> GetLatestEntityPersistInfo(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, Guid entityId)
        {
            throw new NotImplementedException();
        }

        public override Task<IAsyncEnumerable<ConnectorLatestEntityPersistInfo>> GetLatestEntityPersistInfos(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            throw new NotImplementedException();
        }

        //public override async Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId,
        //    string containerName, string originEntityCode, IEnumerable<string> edges)
        //{
        //    var data = new Dictionary<string, object>
        //    {
        //        {"ProviderDefinitionId", providerDefinitionId.ToString()},
        //        {"ContainerName", containerName},
        //        {"OriginEntityCode", originEntityCode},
        //        {"Edges", edges}
        //    };

        //    var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
        //    var configurations = new AzureDataLakeConnectorJobData(connection.Authentication, $"{containerName}.edges");

        //    lock (_cacheLock)
        //    {
        //        _cachingService.AddItem(data, configurations).GetAwaiter().GetResult();
        //        var count = _cachingService.Count().GetAwaiter().GetResult();

        //        if (count >= _cacheRecordsThreshold)
        //        {
        //            Flush();
        //        }
        //    }
        //}

        public override async Task<ConnectionVerificationResult> VerifyConnection(ExecutionContext executionContext, IReadOnlyDictionary<string, object> config)
        {
            await _client.EnsureDataLakeDirectoryExist(new AzureDataLakeConnectorJobData(config.ToDictionary(x => x.Key, x => x.Value)));

            return new ConnectionVerificationResult(true);
        }

        private void Flush((IReadOnlyConnectorEntityData, AzureDataLakeConnectorJobData)[] obj)
        {
            if (obj == null)
            {
                return;
            }

            if (obj.Length == 0)
            {
                return;
            }

            var configuration = obj[0].Item2;  // all connection data should be the same in the batch so use the first

            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None,
                Formatting = Formatting.Indented,
            };

            var content = JsonConvert.SerializeObject(obj.Select(x => x.Item1), settings);

            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm-ss");
            var fileName = $"{configuration.ContainerName}.{timestamp}.json";

            _client.SaveData(configuration, content, fileName).GetAwaiter().GetResult();
        }

        //private void Flush()
        //{
        //    lock (_cacheLock)
        //    {
        //        var itemsCount = _cachingService.Count().GetAwaiter().GetResult();
        //        if (itemsCount == 0)
        //        {
        //            return;
        //        }

        //        var cachedItems = _cachingService.GetItems().GetAwaiter().GetResult();
        //        var cachedItemsByConfigurations = cachedItems.GroupBy(pair => pair.Value).ToList();

        //        var settings = new JsonSerializerSettings
        //        {
        //            TypeNameHandling = TypeNameHandling.None,
        //            Formatting = Formatting.Indented,
        //        };

        //        foreach (var group in cachedItemsByConfigurations)
        //        {
        //            var configuration = group.Key;
        //            var content = JsonConvert.SerializeObject(group.Select(g => g.Key), settings);

        //            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm-ss");
        //            var fileName = $"{configuration.ContainerName}.{timestamp}.json";

        //            ActionExtensions.ExecuteWithRetry(() =>
        //            {
        //                _client.SaveData(configuration, content, fileName).GetAwaiter().GetResult();
        //            });


        //            _cachingService.Clear(configuration).GetAwaiter().GetResult();
        //        }
        //    }
        //}

        public override Task CreateContainer(ExecutionContext executionContext, Guid connectorProviderDefinitionId, IReadOnlyCreateContainerModelV2 model)
        {
            return Task.CompletedTask;
        }

        public override Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            //lock (_cacheLock)
            //{
            //    try
            //    {
            //        Flush();
            //    }
            //    catch (Exception ex)
            //    {
            //        _logger.LogError(ex, $"{nameof(AzureDataLakeConnector)} fails to save entities before reprocessing");
            //        _cachingService.Clear().GetAwaiter().GetResult();
            //    }
            //}

            return Task.CompletedTask;
        }

        public override Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext,
            Guid providerDefinitionId)
        {
            _logger.LogInformation($"AzureDataLakeConnector.GetContainers: entry");

            throw new NotImplementedException(nameof(GetContainers));
        }

        //public override Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext,
        //    Guid providerDefinitionId, string containerId)
        //{
        //    _logger.LogInformation($"AzureDataLakeConnector.GetDataTypes: entry");

        //    throw new NotImplementedException(nameof(GetDataTypes));
        //}

        public override async Task EmptyContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            _logger.LogInformation($"AzureDataLakeConnector.EmptyContainer: entry");

            throw new NotImplementedException(nameof(EmptyContainer));
        }

        public override async Task RenameContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, string oldContainerName)
        {
            _logger.LogInformation($"AzureDataLakeConnector.RenameContainer: entry");

            throw new NotImplementedException(nameof(RenameContainer));
        }

        public override Task<string> GetValidMappingDestinationPropertyName(ExecutionContext executionContext, Guid connectorProviderDefinitionId,
            string propertyName)
        {
            return Task.FromResult(propertyName);
        }

        public override Task<string> GetValidContainerName(ExecutionContext executionContext, Guid connectorProviderDefinitionId, string containerName)
        {
            return Task.FromResult(containerName);
        }

        public override IReadOnlyCollection<StreamMode> GetSupportedModes()
        {
            return new[] { StreamMode.Sync };
        }

        public override async Task RemoveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            _logger.LogInformation($"AzureDataLakeConnector.RemoveContainer: entry");

            throw new NotImplementedException(nameof(RemoveContainer));
        }

        public virtual async Task<IConnectorConnectionV2> GetAuthenticationDetails(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            return await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, providerDefinitionId);
        }
    }
}
