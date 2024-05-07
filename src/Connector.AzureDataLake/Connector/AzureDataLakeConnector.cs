using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

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

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeConnector : ConnectorBaseV2
    {
        private readonly ILogger<AzureDataLakeConnector> _logger;
        private readonly IAzureDataLakeClient _client;
        private readonly PartitionedBuffer<AzureDataLakeConnectorJobData, string> _buffer;
        
        private static readonly Type[] DateAndTimeTypes = new[]
        {
            typeof(DateTime),
            typeof(DateTimeOffset),
            typeof(DateOnly),
            typeof(TimeOnly)
        };

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
            var data = connectorEntityData.Properties.ToDictionary(property => property.Name, property => property.Value);
            data.Add(nameof(IReadOnlyConnectorEntityData.EntityId), connectorEntityData.EntityId);
            data.Add("PersistHash", connectorEntityData.PersistInfo?.PersistHash);
            data.Add("PersistVersion", connectorEntityData.PersistInfo?.PersistVersion);
            data.Add("OriginEntityCode", connectorEntityData.OriginEntityCode?.ToString());
            data.Add("EntityType", connectorEntityData.EntityType?.ToString());
            data.Add("Codes", connectorEntityData.EntityCodes.SafeEnumerate().Select(code => code.ToString()));
            data["ProviderDefinitionId"] = providerDefinitionId;
            data["ContainerName"] = containerName;

            // end match previous version of the connector
            if (streamModel.ExportIncomingEdges)
            {
                data.Add("IncomingEdges", connectorEntityData.IncomingEdges.SafeEnumerate());
            }
            if (streamModel.ExportOutgoingEdges)
            {
                data.Add("OutgoingEdges", connectorEntityData.OutgoingEdges.SafeEnumerate());
            }

            if (configurations.EnableBuffer)
            {
                return await PutToBuffer(streamModel, connectorEntityData, configurations, data);
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
                            EntityId UNIQUEIDENTIFIER NOT NULL,
                            {string.Join(string.Empty, propertiesColumns.Select(prop => $"{prop},\n    "))}
                            [ValidFrom] DATETIME2 GENERATED ALWAYS AS ROW START,
                            [ValidTo] DATETIME2 GENERATED ALWAYS AS ROW END,
                            PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo),
                            CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED (EntityId)
                        ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.[{tableName}_History]));
                        """, connection);
            command.CommandType = CommandType.Text;
            _ = await command.ExecuteNonQueryAsync();
        }

        private async Task<SaveResult> PutToBuffer(IReadOnlyStreamModel streamModel, IReadOnlyConnectorEntityData connectorEntityData, AzureDataLakeConnectorJobData configurations, Dictionary<string, object> data)
        {
            if (streamModel.Mode == StreamMode.Sync)
            {
                await using var connection = new SqlConnection(configurations.BufferConnectionString);
                await connection.OpenAsync();
                var tableName = GetBufferTableName(streamModel.Id);

                var propertyKeys = data.Keys.Except(new[] { nameof(IReadOnlyConnectorEntityData.EntityId) }).OrderBy(key => key).ToList();

                await EnsureTableExists(connection, tableName, propertyKeys);

                if (connectorEntityData.ChangeType == VersionChangeType.Removed)
                {
                    var command = new SqlCommand($"DELETE FROM [{tableName}] WHERE EntityId = @EntityId", connection);
                    command.CommandType = CommandType.Text;
                    command.Parameters.Add(new SqlParameter("@EntityId", connectorEntityData.EntityId));
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
                        IF EXISTS (SELECT 1 FROM [{tableName}] WHERE EntityId = @EntityId)  
                        BEGIN  
                        	UPDATE [{tableName}]   
                        	SET
                                {string.Join(",\n        ", propertyKeys.Select((key, index) => $"[{key}] = @p{index}"))}  
                        	WHERE EntityId = @EntityId;  
                        END  
                        ELSE  
                        BEGIN  
                        	INSERT INTO [{tableName}] (EntityId{string.Join(string.Empty, propertyKeys.Select(key => $", [{key}]"))})
                            VALUES(@EntityId{string.Join(string.Empty, propertyKeys.Select((_, index) => $", @p{index}"))})
                        END
                        """, connection);
                    command.CommandType = CommandType.Text;
                    command.Parameters.Add(new SqlParameter("@EntityId", connectorEntityData.EntityId));

                    for (var i = 0; i < propertyKeys.Count; i++)
                    {
                        command.Parameters.Add(new SqlParameter($"@p{i}", GetDbValue(data[propertyKeys[i]])));
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

        private static string GetBufferTableName(Guid streamId)
        {
            return $"Stream_{streamId}";
        }

        private static object GetDbValue(object value)
        {
            if (value == null)
            {
                return DBNull.Value;
            }

            var type = value.GetType();
           
            if (type == typeof(string))
            {
                return value.ToString();
            }

            if (DateAndTimeTypes.Contains(type)
                && value is IFormattable formattable)
            {
                return formattable.ToString("o", CultureInfo.InvariantCulture);
            }

            if (value is TimeSpan timeSpan)
            {
                return timeSpan.ToString("c", CultureInfo.InvariantCulture);
            }

            if (value is Guid guid)
            {
                return guid.ToString("D", CultureInfo.InvariantCulture);
            }

            return JsonUtility.Serialize(value);
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
            try
            {
                var jobData = new AzureDataLakeConnectorJobData(config.ToDictionary(x => x.Key, x => x.Value));
                await _client.EnsureDataLakeDirectoryExist(jobData);

                if (jobData.EnableBuffer)
                {
                    if (string.IsNullOrWhiteSpace(jobData.BufferConnectionString))
                    {
                        return new ConnectionVerificationResult(false, $"Buffer connection string must be provided when buffer is enabled.");
                    }
                    await using var connection = new SqlConnection(jobData.BufferConnectionString);
                    await connection.OpenAsync();
                    await using var connectionAndTransaction = await connection.BeginTransactionAsync();
                    var connectionIsOpen = connectionAndTransaction.Connection.State == ConnectionState.Open;
                    await connectionAndTransaction.DisposeAsync();

                    if (!connectionIsOpen)
                    {
                        return new ConnectionVerificationResult(false, "Failed to connect to the buffer storage.");
                    }
                }
                return new ConnectionVerificationResult(true);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error verifying connection");
                return new ConnectionVerificationResult(false, e.Message);
            }
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
        
        public override async Task CreateContainer(ExecutionContext executionContext, Guid connectorProviderDefinitionId, IReadOnlyCreateContainerModelV2 model)
        {
            await Task.CompletedTask;
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
                IF EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
                ALTER TABLE dbo.[{tableName}]  SET ( SYSTEM_VERSIONING = Off )

                DROP TABLE IF EXISTS [{tableName}];
                DROP TABLE IF EXISTS [{tableName}_History];
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
