using CluedIn.Connector.AzureDataLake.Extensions;
using CluedIn.Connector.AzureDataLake.Helpers;
using CluedIn.Core;
using CluedIn.Core.Configuration;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.DataStore.Entities.MicroServices;
using CluedIn.Core.Processing;
using CluedIn.Core.Streams.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Parquet.Schema;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using static CluedIn.Connector.AzureDataLake.AzureDataLakeConstants;
using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeConnector : ConnectorBaseV2
    {
        private readonly ILogger<AzureDataLakeConnector> _logger;
        private readonly IAzureDataLakeClient _client;
        private readonly PartitionedBuffer<AzureDataLakeConnectorJobData, string> _buffer;
        private readonly DbContextOptions<MicroServiceEntities> _dbContextOptions;
        private readonly Dictionary<AzureDataLakeConnectorJobData, ParquetSchema> _parquetSchemas;

        public AzureDataLakeConnector(
            ILogger<AzureDataLakeConnector> logger,
            IAzureDataLakeClient client,
            IAzureDataLakeConstants constants,
            DbContextOptions<MicroServiceEntities> dbContextOptions)
            : base(constants.ProviderId, false)
        {
            _logger = logger;
            _client = client;


            var cacheRecordsThreshold = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheRecordsThresholdKeyName, constants.CacheRecordsThresholdDefaultValue);
            var backgroundFlushMaxIdleDefaultValue = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheSyncIntervalKeyName, constants.CacheSyncIntervalDefaultValue);

            _buffer = new PartitionedBuffer<AzureDataLakeConnectorJobData, string>(1000, //cacheRecordsThreshold,
                backgroundFlushMaxIdleDefaultValue, Flush);
            _dbContextOptions = dbContextOptions;

            _parquetSchemas = new();
        }

        ~AzureDataLakeConnector()
        {
            _buffer.Dispose();
        }

        public override Task VerifyExistingContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            return Task.FromResult(0);
        }

        public override async Task<SaveResult> StoreData(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, IReadOnlyConnectorEntityData connectorEntityData)
        {
            string outputFormat = null;
            if (streamModel.ConnectorProperties.ContainsKey(ExtendedConfigurationProperties.OutputFormat))
                outputFormat = streamModel.ConnectorProperties[ExtendedConfigurationProperties.OutputFormat].ToString();

            var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
            var containerName = streamModel.ContainerName;

            var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            var configurations = new AzureDataLakeConnectorJobData(connection.Authentication.ToDictionary(x => x.Key, x => x.Value), containerName,
                streamModel.ConnectorProperties);

            // matching output format of previous version of the connector
            var data = connectorEntityData.Properties.ToDictionary(x => x.Name, x => x.Value);
            data.Add("Id", connectorEntityData.EntityId);

            if (connectorEntityData.PersistInfo != null)
            {
                data.Add("PersistHash", connectorEntityData.PersistInfo.PersistHash);
            }

            if (connectorEntityData.OriginEntityCode != null)
            {
                data.Add("OriginEntityCode", connectorEntityData.OriginEntityCode.ToString());
            }

            if (connectorEntityData.EntityType != null)
            {
                data.Add("EntityType", connectorEntityData.EntityType.ToString());
            }
            data.Add("Codes", connectorEntityData.EntityCodes.SafeEnumerate().Select(c => c.ToString()).ToArray());

            data["ProviderDefinitionId"] = providerDefinitionId;
            data["ContainerName"] = containerName;
            // end match previous version of the connector

            var serializer = JsonUtility.CreateDefaultSerializer(settings =>
            {
                settings.Formatting = Formatting.Indented;
                settings.TypeNameHandling = TypeNameHandling.None;
            });

            if (connectorEntityData.OutgoingEdges.SafeEnumerate().Any())
            {
                if (outputFormat == "JSON")
                    data.Add("OutgoingEdges", connectorEntityData.OutgoingEdges);
                else
                {
                    //var edges = JsonUtility.Serialize(connectorEntityData.OutgoingEdges, serializer);
                    //data.Add("OutgoingEdges", JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(edges));
                }
            }

            if (connectorEntityData.IncomingEdges.SafeEnumerate().Any())
            {
                if (outputFormat == "JSON")
                    data.Add("IncomingEdges", connectorEntityData.IncomingEdges);
                else
                {
                    //var edges = JsonUtility.Serialize(connectorEntityData.IncomingEdges, serializer);
                    //data.Add("IncomingEdges", JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(edges));
                }
            }

            if (streamModel.Mode == StreamMode.Sync)
            {
                var filePathAndName = $"{connectorEntityData.EntityId.ToString()[..2]}/{connectorEntityData.EntityId.ToString().Substring(2, 2)}/{connectorEntityData.EntityId}.{outputFormat.GetFileExtension()}";

                if (connectorEntityData.ChangeType == VersionChangeType.Removed)
                {
                    await _client.DeleteFile(configurations, filePathAndName);
                }
                else
                {
                    Stream stream = null;
                    if (outputFormat == "JSON")
                        stream = data.ToJsonStream();
                    else
                    {
                        var lExecuted = false;
                        var parquetSchema = _parquetSchemas.TryAddOrUpdate(configurations,
                            () =>
                            {
                                lExecuted = true;
                                return ParquetHelper.GenerateSchema(data, connectorEntityData.Properties);
                            });

                        if (!lExecuted)
                        {
                            parquetSchema.DataFields.ForEach(field =>
                            {
                                data[field.Name] = ParquetHelper.ValidateData(field.ClrType, data[field.Name]);
                            });
                        }

                        var parquetTable = new Parquet.Rows.Table(parquetSchema)
                        {
                            new Parquet.Rows.Row(data.Values.ToArray())
                        };

                        stream = await ParquetHelper.ToStream(parquetTable);
                    }

                    await _client.SaveData(configurations, stream, filePathAndName);
                }
            }
            else
            {
                data.Add("ChangeType", connectorEntityData.ChangeType.ToString());

                string item;
                if (outputFormat == "JSON")
                {
                    item = JsonUtility.Serialize(data, serializer);
                }
                else
                {
                    var lExecuted = false;
                    var parquetSchema = _parquetSchemas.TryAddOrUpdate(configurations,
                        () =>
                        {
                            lExecuted = true;
                            return ParquetHelper.GenerateSchema(data, connectorEntityData.Properties);
                        });

                    if (!lExecuted)
                    {
                        parquetSchema.DataFields.ForEach(field =>
                        {
                            data[field.Name] = ParquetHelper.ValidateData(field.ClrType, data[field.Name]);
                        });
                    }

                    item = StreamHelper.ConvertToStreamString(data.Values.ToArray());
                }

                await _buffer.Add(configurations, item);
            }

            return SaveResult.Success;
        }

        public override Task<ConnectorLatestEntityPersistInfo> GetLatestEntityPersistInfo(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, Guid entityId)
        {
            throw new NotImplementedException();
        }

        public override Task<IAsyncEnumerable<ConnectorLatestEntityPersistInfo>> GetLatestEntityPersistInfos(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            throw new NotImplementedException();
        }

        public override async Task<ConnectionVerificationResult> VerifyConnection(ExecutionContext executionContext, IReadOnlyDictionary<string, object> config)
        {
            await _client.EnsureDataLakeDirectoryExist(new AzureDataLakeConnectorJobData(config.ToDictionary(x => x.Key, x => x.Value)));

            return new ConnectionVerificationResult(true);
        }

        private void Flush(AzureDataLakeConnectorJobData configuration, string[] entityData)
        {
            if (entityData == null)
            {
                return;
            }

            if (entityData.Length == 0)
            {
                return;
            }

            string outputFormat = null;
            if (configuration.ConnectorProperties.ContainsKey(ExtendedConfigurationProperties.OutputFormat))
                outputFormat = configuration.ConnectorProperties[ExtendedConfigurationProperties.OutputFormat].ToString();

            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm-ss.fffffff");
            var fileName = $"{configuration.ContainerName}.{timestamp}.{outputFormat.GetFileExtension()}";

            Stream stream = null;
            if (outputFormat == "JSON")
            {
                var datas = entityData.Select(s => JsonConvert.DeserializeObject<Dictionary<string, object>>(s)).ToArray();

                stream = datas.ToJsonStream();
            }
            else
            {
                var parquetSchema = _parquetSchemas[configuration];
                var parquetDatas = entityData.Select(s => new Parquet.Rows.Row(StreamHelper.ConvertToObject<object[]>(s)));
                var parquetTable = new Parquet.Rows.Table(parquetSchema);
                parquetTable.AddRange(parquetDatas);

                stream = ParquetHelper.ToStream(parquetTable).GetAwaiter().GetResult();
            }

            _client.SaveData(configuration, stream, fileName).GetAwaiter().GetResult();
        }

        public override Task CreateContainer(ExecutionContext executionContext, Guid connectorProviderDefinitionId, IReadOnlyCreateContainerModelV2 model)
        {
            return Task.CompletedTask;
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            await _buffer.Flush();
        }

        public override Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext,
            Guid providerDefinitionId)
        {
            _logger.LogInformation($"AzureDataLakeConnector.GetContainers: entry");

            throw new NotImplementedException(nameof(GetContainers));
        }

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
            return new[] { StreamMode.Sync, StreamMode.EventStream };
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
