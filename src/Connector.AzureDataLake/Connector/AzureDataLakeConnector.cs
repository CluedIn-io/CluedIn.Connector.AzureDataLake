using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;

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

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeConnector : ConnectorBaseV2
    {
        private readonly ILogger<AzureDataLakeConnector> _logger;
        private readonly IAzureDataLakeClient _client;
        private readonly PartitionedBuffer<AzureDataLakeConnectorJobData, string> _buffer;

        // TODO: Handle ushort, ulong, uint
        private static readonly Dictionary<Type, string> DotNetToSqlTypeMap = new ()
        {
            [typeof(bool)] = "BIT",
            [typeof(byte)] = "TINYINT",
            [typeof(short)] = "SMALLINT",
            [typeof(int)] = "INT",
            [typeof(long)] = "BIGINT",
            [typeof(float)] = "REAL",
            [typeof(double)] = "FLOAT",
            [typeof(decimal)] = "DECIMAL",
            [typeof(DateTime)] = "DATETIME2",
            [typeof(DateTimeOffset)] = "DATETIMEOFFSET",
            [typeof(TimeSpan)] = "TIME",
            [typeof(Guid)] = "UNIQUEIDENTIFIER",
            [typeof(string)] = "NVARCHAR(MAX)"
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

        public override async Task<SaveResult> StoreData(
            ExecutionContext executionContext,
            IReadOnlyStreamModel streamModel,
            IReadOnlyConnectorEntityData connectorEntityData)
        {
            var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
            var containerName = streamModel.ContainerName;
            var jobData = await CreateJobData(executionContext, providerDefinitionId, containerName);

            // matching output format of previous version of the connector
            var data = connectorEntityData.Properties.ToDictionary(property => property.Name, property => property.Value);
            var dataValueTypes = new Dictionary<string, Type>();
            foreach(var property in connectorEntityData.Properties)
            {
                var type = property.GetDataType();
                var nullableUnderlyingType = Nullable.GetUnderlyingType(type);
                if (nullableUnderlyingType != null)
                {
                    type = nullableUnderlyingType;
                }

                dataValueTypes.Add(property.Name, type);
            }

            void AddToData<T>(string key, T value)
            {
                data.Add(key, value);
                dataValueTypes.Add(key, typeof(T));
            }
            AddToData(AzureDataLakeConstants.IdKey, connectorEntityData.EntityId);
            AddToData("PersistHash", connectorEntityData.PersistInfo?.PersistHash);
            AddToData("PersistVersion", connectorEntityData.PersistInfo?.PersistVersion);
            AddToData("OriginEntityCode", connectorEntityData.OriginEntityCode?.ToString());
            AddToData("EntityType", connectorEntityData.EntityType?.ToString());
            AddToData("Codes", connectorEntityData.EntityCodes.SafeEnumerate().Select(code => code.ToString()));
            AddToData("ProviderDefinitionId", providerDefinitionId);
            AddToData("ContainerName", containerName);

            // end match previous version of the connector
            if (streamModel.ExportOutgoingEdges)
            {
                AddToData("OutgoingEdges", connectorEntityData.OutgoingEdges.SafeEnumerate());
            }
            if (streamModel.ExportIncomingEdges)
            {
                AddToData("IncomingEdges", connectorEntityData.IncomingEdges.SafeEnumerate());
            }

            if (jobData.IsStreamCacheEnabled && streamModel.Mode == StreamMode.Sync)
            {
                return await WriteToCacheTable(streamModel, connectorEntityData, jobData, data, dataValueTypes);
            }
            else
            {
                return await WriteToOutputImmediately(streamModel, connectorEntityData, jobData, data, dataValueTypes);
            }
        }

        public virtual async Task<AzureDataLakeConnectorJobData> CreateJobData(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName)
        {
            return await AzureDataLakeConnectorJobData.Create(executionContext, providerDefinitionId, containerName);
        }

        private async Task EnsureTableExists(SqlConnection connection, string tableName, ICollection<string> propertyKeys, Dictionary<string, Type> dataValueTypes)
        {
            var propertiesColumns = propertyKeys.Select(key =>
            {
                var type = dataValueTypes[key];
                var sqlType = DotNetToSqlTypeMap.TryGetValue(type, out var sqlDbType) ? sqlDbType : DotNetToSqlTypeMap[typeof(string)];
                return $"[{key}] {sqlType}";
            });
            var command = new SqlCommand(
                $"""
                        IF NOT EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
                        CREATE TABLE [{tableName}] (
                            {AzureDataLakeConstants.IdKey} UNIQUEIDENTIFIER NOT NULL,
                            {string.Join(string.Empty, propertiesColumns.Select(prop => $"{prop},\n    "))}
                            [ValidFrom] DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN,
                            [ValidTo] DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN,
                            PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo),
                            CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ({AzureDataLakeConstants.IdKey})
                        ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.[{tableName}_History]));
                        """, connection);
            command.CommandType = CommandType.Text;
            _ = await command.ExecuteNonQueryAsync();
        }

        private async Task<SaveResult> WriteToCacheTable(IReadOnlyStreamModel streamModel, IReadOnlyConnectorEntityData connectorEntityData, AzureDataLakeConnectorJobData configurations, Dictionary<string, object> data, Dictionary<string, Type> dataValueTypes)
        {
            if (streamModel.Mode == StreamMode.Sync)
            {
                var syncItem = new SyncItem(streamModel.Id, connectorEntityData.EntityId, connectorEntityData.ChangeType, data, dataValueTypes);
                await using var connection = new SqlConnection(configurations.StreamCacheConnectionString);
                await connection.OpenAsync();
                await WriteToCacheTable(connection, syncItem);
            }
            else
            {
                throw new NotSupportedException($"Buffer mode is not supported with '{StreamMode.EventStream}' mode.");
            }

            return SaveResult.Success;
        }

        private async Task WriteToCacheTable(
            SqlConnection connection,
            SyncItem syncItem)
        {
            var tableName = GetCacheTableName(syncItem.StreamId);
            var propertyKeys = syncItem.Data.Keys.Except(new[] { AzureDataLakeConstants.IdKey }).OrderBy(key => key).ToList();

            try
            {
                await ExecuteWrite(connection, syncItem, tableName, propertyKeys);
            }
            catch (SqlException ex) when (GetIsTableNotFoundException(ex))
            {
                await EnsureTableExists(connection, tableName, propertyKeys, syncItem.DataValueTypes);
                await ExecuteWrite(connection, syncItem, tableName, propertyKeys);
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, "Failed to process Entity with Id {EntityId}.", syncItem.EntityId);
                throw;
            }

            static async Task ExecuteWrite(SqlConnection connection, SyncItem syncItem, string tableName, List<string> propertyKeys)
            {
                if (syncItem.ChangeType == VersionChangeType.Removed)
                {
                    var command = new SqlCommand($"DELETE FROM [{tableName}] WHERE {AzureDataLakeConstants.IdKey} = @{AzureDataLakeConstants.IdKey}", connection);
                    command.CommandType = CommandType.Text;
                    command.Parameters.Add(new SqlParameter($"@{AzureDataLakeConstants.IdKey}", syncItem.EntityId));
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
                        IF EXISTS (SELECT 1 FROM [{tableName}] WHERE {AzureDataLakeConstants.IdKey} = @{AzureDataLakeConstants.IdKey})  
                        BEGIN  
                        	UPDATE [{tableName}]   
                        	SET
                                {string.Join(",\n        ", propertyKeys.Select((key, index) => $"[{key}] = @p{index}"))}  
                        	WHERE {AzureDataLakeConstants.IdKey} = @{AzureDataLakeConstants.IdKey};  
                        END  
                        ELSE  
                        BEGIN  
                        	INSERT INTO [{tableName}] ({AzureDataLakeConstants.IdKey}{string.Join(string.Empty, propertyKeys.Select(key => $", [{key}]"))})
                            VALUES(@{AzureDataLakeConstants.IdKey}{string.Join(string.Empty, propertyKeys.Select((_, index) => $", @p{index}"))})
                        END
                        """, connection);
                    command.CommandType = CommandType.Text;
                    command.Parameters.Add(new SqlParameter($"@{AzureDataLakeConstants.IdKey}", syncItem.EntityId));

                    for (var i = 0; i < propertyKeys.Count; i++)
                    {
                        command.Parameters.Add(new SqlParameter($"@p{i}", GetDatabaseValue(syncItem.Data[propertyKeys[i]])));
                    }

                    var rowsAffected = await command.ExecuteNonQueryAsync();
                    if (rowsAffected != 1)
                    {
                        throw new ApplicationException($"Rows affected for insertion of is not 1, it is {rowsAffected}.");
                    }
                }
            }
        }

        private static bool GetIsTableNotFoundException(SqlException ex)
        {
            return ex.Number == 208;
        }

        private static string GetCacheTableName(Guid streamId)
        {
            return CacheTableHelper.GetCacheTableName(streamId);
        }

        private static object GetDatabaseValue(object value)
        {
            if (value == null)
            {
                return DBNull.Value;
            }

            if (DotNetToSqlTypeMap.ContainsKey(value.GetType()))
            {
                return value;
            }

            return JsonUtility.Serialize(value);
        }

        private async Task<SaveResult> WriteToOutputImmediately(IReadOnlyStreamModel streamModel, IReadOnlyConnectorEntityData connectorEntityData, AzureDataLakeConnectorJobData configurations, Dictionary<string, object> data, Dictionary<string, Type> dataValueTypes)
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
                var jobData = await AzureDataLakeConnectorJobData.Create(executionContext, config.ToDictionary(config => config.Key, config => config.Value));
                await _client.EnsureDataLakeDirectoryExist(jobData);

                if (jobData.IsStreamCacheEnabled)
                {
                    if (string.IsNullOrWhiteSpace(jobData.StreamCacheConnectionString))
                    {
                        return new ConnectionVerificationResult(false, $"Buffer connection string must be valid when buffer is enabled.");
                    }
                    await using var connection = new SqlConnection(jobData.StreamCacheConnectionString);
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
            await DeleteCacheTableIfExists(executionContext, streamModel);
        }

        public override Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext,
            Guid providerDefinitionId)
        {
            _logger.LogInformation($"AzureDataLakeConnector.GetContainers: entry");

            throw new NotImplementedException(nameof(GetContainers));
        }

        public override Task EmptyContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            _logger.LogInformation($"AzureDataLakeConnector.EmptyContainer: entry");

            throw new NotImplementedException(nameof(EmptyContainer));
        }

        public override Task RenameContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, string oldContainerName)
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

        public override Task RemoveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            _logger.LogInformation($"AzureDataLakeConnector.RemoveContainer: entry");

            throw new NotImplementedException(nameof(RemoveContainer));
        }

        private async Task DeleteCacheTableIfExists(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
            var containerName = streamModel.ContainerName;

            var jobData = await AzureDataLakeConnectorJobData.Create(executionContext, providerDefinitionId, containerName);
            await using var connection = new SqlConnection(jobData.StreamCacheConnectionString);
            await connection.OpenAsync();
            var tableName = GetCacheTableName(streamModel.Id);
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

        private record SyncItem(
            Guid StreamId,
            Guid EntityId,
            VersionChangeType ChangeType,
            IDictionary<string, object> Data,
            Dictionary<string, Type> DataValueTypes);
    }
}
