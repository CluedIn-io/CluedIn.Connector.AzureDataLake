using Castle.MicroKernel.Registration;

using CluedIn.Core;
using CluedIn.Core.Configuration;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Processing;
using CluedIn.Core.Streams.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using static CluedIn.Core.Constants.Configuration;

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeConnector : ConnectorBaseV2
    {
        private readonly ILogger<AzureDataLakeConnector> _logger;
        private readonly IAzureDataLakeClient _client;
        private readonly PartitionedBuffer<AzureDataLakeConnectorJobData, string> _buffer;

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

            _buffer = new PartitionedBuffer<AzureDataLakeConnectorJobData, string>(cacheRecordsThreshold,
                backgroundFlushMaxIdleDefaultValue, Flush);
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
            var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
            var containerName = streamModel.ContainerName;

            var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            var configurations = new AzureDataLakeConnectorJobData(connection.Authentication.ToDictionary(x => x.Key, x => x.Value), containerName);

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
            data.Add("Codes", connectorEntityData.EntityCodes.SafeEnumerate().Select(c => c.ToString()));

            data["ProviderDefinitionId"] = providerDefinitionId;
            data["ContainerName"] = containerName;
            // end match previous version of the connector

            if (connectorEntityData.OutgoingEdges.SafeEnumerate().Any())
            {
                data.Add("OutgoingEdges", connectorEntityData.OutgoingEdges);
            }

            if (connectorEntityData.IncomingEdges.SafeEnumerate().Any())
            {
                data.Add("IncomingEdges", connectorEntityData.IncomingEdges);
            }

            if (configurations.EnableBuffer)
            {
                return await WriteWithBuffer(streamModel, connectorEntityData, configurations, data);
            }
            else
            {
                return await WriteWithoutBuffer(streamModel, connectorEntityData, configurations, data);
            }
        }

        private async Task EnsureTableExists(SqlConnection connection, string tableName, ICollection<string> propertyKeys)
        {
            var propertiesColumns = propertyKeys.Select(key => $"[{key}] NVARCHAR(MAX)");
            var command = new SqlCommand(
                $"""
                        IF NOT EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
                        CREATE TABLE [{tableName}] (
                            Id UNIQUEIDENTIFIER NOT NULL,
                            {string.Join(string.Empty, propertiesColumns.Select(prop => $"{prop},\n    "))}
                            [ValidFrom] DATETIME2 GENERATED ALWAYS AS ROW START,
                            [ValidTo] DATETIME2 GENERATED ALWAYS AS ROW END,
                            PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo),
                            CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED (Id)
                        ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.[{tableName}_History]));
                        """, connection);
            command.CommandType = CommandType.Text;
            _ = await command.ExecuteNonQueryAsync();
            //command.Parameters.Add(new SqlParameter("@EmployeeID", employeeID));
            //command.CommandTimeout = 5;
        }
        private async Task<SaveResult> WriteWithBuffer(IReadOnlyStreamModel streamModel, IReadOnlyConnectorEntityData connectorEntityData, AzureDataLakeConnectorJobData configurations, Dictionary<string, object> data)
        {
            if (streamModel.Mode == StreamMode.Sync)
            {
                await using var connection = new SqlConnection(configurations.BufferConnectionString);
                await connection.OpenAsync();
                var tableName = $"Stream_{streamModel.Id}";

                var propertyKeys = connectorEntityData.Properties
                    .Select(prop => prop.Name)
                    .OrderBy(key => key)
                    .ToList();

                //var propertyKeys = data.Keys.OrderBy(key => key).ToList();
                await EnsureTableExists(connection, tableName, propertyKeys);

                if (connectorEntityData.ChangeType == VersionChangeType.Removed)
                {
                    var command = new SqlCommand($"DELETE FROM [{tableName}] WHERE Id = @Id", connection);
                    command.CommandType = CommandType.Text;
                    command.Parameters.Add(new SqlParameter("@Id", connectorEntityData.EntityId));
                    var rowsAffected = await command.ExecuteNonQueryAsync();
                    if (rowsAffected != 1)
                    {
                        throw new ApplicationException($"Rows affected for deletion is not 1, it is {rowsAffected}.");
                    }
                }
                else
                {
                    var command = new SqlCommand(
                        $"""
                        IF EXISTS (SELECT 1 FROM [{tableName}] WHERE Id = @Id)  
                        BEGIN  
                        	UPDATE [{tableName}]   
                        	SET
                                {string.Join(",\n        ", propertyKeys.Select((key, index) => $"[{key}] = @p{index}"))}  
                        	WHERE Id = @Id;  
                        END  
                        ELSE  
                        BEGIN  
                        	INSERT INTO [{tableName}] (Id{string.Join(string.Empty, propertyKeys.Select(key => $", [{key}]"))})
                            VALUES(@Id{string.Join(string.Empty, propertyKeys.Select((_, index) => $", @p{index}"))})
                        END
                        """, connection);
                    command.CommandType = CommandType.Text;
                    command.Parameters.Add(new SqlParameter("@Id", connectorEntityData.EntityId));

                    for (var i = 0; i < propertyKeys.Count; i++)
                    {
                        // TODO: Dictionary
                        var property = connectorEntityData.Properties.Single(vocabKey => vocabKey.Name == propertyKeys[i]);

                        // TODO: Remove leading & trailing quotes from string
                        command.Parameters.Add(new SqlParameter($"@p{i}", JsonUtility.Serialize(property.Value)));
                    }


                    var rowsAffected = await command.ExecuteNonQueryAsync();
                    if (rowsAffected != 1)
                    {
                        throw new ApplicationException($"Rows affected for insertion is not 1, it is {rowsAffected}.");
                    }
                }
            }
            else
            {
                throw new NotSupportedException($"Buffer mode is not supported with '{StreamMode.EventStream}' mode.");
            }

            return SaveResult.Success;
        }

        private async Task<SaveResult> WriteWithoutBuffer(IReadOnlyStreamModel streamModel, IReadOnlyConnectorEntityData connectorEntityData, AzureDataLakeConnectorJobData configurations, Dictionary<string, object> data)
        {
            if (streamModel.Mode == StreamMode.Sync)
            {
                var filePathAndName = $"{connectorEntityData.EntityId.ToString().Substring(0, 2)}/{connectorEntityData.EntityId.ToString().Substring(2, 2)}/{connectorEntityData.EntityId}.json";

                if (connectorEntityData.ChangeType == VersionChangeType.Removed)
                {
                    await _client.DeleteFile(configurations, filePathAndName);
                }
                else
                {
                    var settings = new JsonSerializerSettings
                    {
                        TypeNameHandling = TypeNameHandling.None,
                        Formatting = Formatting.Indented,
                    };

                    var json = JsonConvert.SerializeObject(data, settings);

                    await _client.SaveData(configurations, json, filePathAndName);

                }
            }
            else
            {
                data.Add("ChangeType", connectorEntityData.ChangeType.ToString());

                var json = JsonConvert.SerializeObject(data);

                await _buffer.Add(configurations, json);
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

            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None,
                Formatting = Formatting.Indented,
            };

            var content = JsonConvert.SerializeObject(entityData.Select(JObject.Parse).ToArray(), settings);

            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm-ss.fffffff");
            var fileName = $"{configuration.ContainerName}.{timestamp}.json";

            _client.SaveData(configuration, content, fileName).GetAwaiter().GetResult();
        }
        
        public override Task CreateContainer(ExecutionContext executionContext, Guid connectorProviderDefinitionId, IReadOnlyCreateContainerModelV2 model)
        {
            return Task.CompletedTask;
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            await _buffer.Flush();
            await DeleteBuffer(executionContext, streamModel);
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

        private async Task DeleteBuffer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
            var containerName = streamModel.ContainerName;

            var authenticationDetails = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            var configurations = new AzureDataLakeConnectorJobData(authenticationDetails.Authentication.ToDictionary(x => x.Key, x => x.Value), containerName);
            var connection = new SqlConnection(configurations.BufferConnectionString);
            await connection.OpenAsync();
            var tableName = $"Stream_{streamModel.Id}";
            var command = new SqlCommand(
                $"""
                ALTER TABLE dbo.[{tableName}]  SET ( SYSTEM_VERSIONING = Off )

                DROP TABLE [{tableName}];
                DROP TABLE [{tableName}_History];
                """, connection);
            command.CommandType = CommandType.Text;
            _ = await command.ExecuteNonQueryAsync();
        }

        public virtual async Task<IConnectorConnectionV2> GetAuthenticationDetails(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            return await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, providerDefinitionId);
        }
    }
}
