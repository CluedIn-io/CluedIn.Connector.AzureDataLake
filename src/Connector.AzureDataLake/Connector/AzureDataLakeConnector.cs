﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

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
        private const string JsonMimeType = "application/json";
        private const int TableCreationLockTimeoutInMillliseconds = 100;
        private readonly ILogger<AzureDataLakeConnector> _logger;
        private readonly IAzureDataLakeClient _client;
        private readonly PartitionedBuffer<AzureDataLakeConnectorJobData, string> _buffer;
        private static readonly JsonSerializerSettings _serializerSettings = GetJsonSerializerSettings();

        // TODO: Handle ushort, ulong, uint
        private static readonly Dictionary<Type, string> _dotNetToSqlTypeMap = new ()
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
                return await WriteToOutputImmediately(streamModel, connectorEntityData, jobData, data);
            }
        }

        public virtual async Task<AzureDataLakeConnectorJobData> CreateJobData(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName)
        {
            return await AzureDataLakeConnectorJobData.Create(executionContext, providerDefinitionId, containerName);
        }

        private async Task EnsureCacheTableExists(
            SqlConnection connection,
            string tableName,
            SyncItem syncItem)
        {
            var propertyKeys = GetPropertyKeysWithoutId(syncItem);
            var propertiesColumns = propertyKeys.Select(key =>
            {
                var type = syncItem.DataValueTypes[key];
                var sqlType = _dotNetToSqlTypeMap.TryGetValue(type, out var sqlDbType) ? sqlDbType : _dotNetToSqlTypeMap[typeof(string)];
                return $"[{key}] {sqlType}";
            });
            var createTableSql = $"""
                IF NOT EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
                CREATE TABLE [{tableName}] (
                    {AzureDataLakeConstants.IdKey} UNIQUEIDENTIFIER NOT NULL,
                    {string.Join(string.Empty, propertiesColumns.Select(prop => $"{prop},\n    "))}
                    [ValidFrom] DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN,
                    [ValidTo] DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN,
                    PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo),
                    CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ({AzureDataLakeConstants.IdKey})
                ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.[{tableName}_History]));
                """;
            var command = new SqlCommand(createTableSql, connection)
            {
                CommandType = CommandType.Text
            };
            _ = await command.ExecuteNonQueryAsync();
            
        }

        private async Task<SaveResult> WriteToCacheTable(
            IReadOnlyStreamModel streamModel,
            IReadOnlyConnectorEntityData connectorEntityData,
            AzureDataLakeConnectorJobData configurations,
            Dictionary<string, object> data,
            Dictionary<string, Type> dataValueTypes)
        {
            if (streamModel.Mode != StreamMode.Sync)
            {
                throw new NotSupportedException($"Buffer mode is only supported with '{StreamMode.Sync}' mode.");
            }

            var syncItem = new SyncItem(streamModel.Id, connectorEntityData.EntityId, connectorEntityData.ChangeType, data, dataValueTypes);
            var tableName = GetCacheTableName(syncItem.StreamId);

            try
            {
                using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                await using var connection = new SqlConnection(configurations.StreamCacheConnectionString);
                await connection.OpenAsync();
                await WriteToCacheTable(connection, syncItem, tableName);
                transactionScope.Complete();
            }
            catch (SqlException writeDataException) when (GetIsTableNotFoundException(writeDataException))
            {
                try
                {
                    _logger.LogDebug("Table {TableName} does not exist. Trying to create and retry.", tableName);
                    using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                    await using var connection = new SqlConnection(configurations.StreamCacheConnectionString);
                    await connection.OpenAsync();
                    
                    var acquiredLock = await TryAcquireTableCreationLock(connection, tableName);
                    if (!acquiredLock)
                    {
                        _logger.LogDebug("Unable to acquire lock for table creation. Table might be in the process of being created.");
                    }
                    else
                    {
                        await EnsureCacheTableExists(connection, tableName, syncItem);
                    }
                    await WriteToCacheTable(connection, syncItem, tableName);
                    transactionScope.Complete();
                }
                catch (Exception ex2)
                {
                    _logger.LogError(ex2, "Failed to process Entity with Id {EntityId}.", syncItem.EntityId);
                    throw;
                }
            }
            catch (Exception ex1)
            {
                _logger.LogError(ex1, "Failed to process Entity with Id {EntityId}.", syncItem.EntityId);
                throw;
            }

            return SaveResult.Success;
        }

        private Task<bool> TryAcquireTableCreationLock(SqlConnection connection, string tableName)
        {
            return DistributedLockHelper.TryAcquireTableCreationLock(
                connection,
                $"{nameof(AzureDataLakeConnector)}_{tableName}",
                TableCreationLockTimeoutInMillliseconds);
        }

        private async Task WriteToCacheTable(
            SqlConnection connection,
            SyncItem syncItem,
            string tableName)
        {
            var propertyKeys = GetPropertyKeysWithoutId(syncItem);
            if (syncItem.ChangeType == VersionChangeType.Removed)
            {
                var deleteCommandText = $"DELETE FROM [{tableName}] WHERE {AzureDataLakeConstants.IdKey} = @{AzureDataLakeConstants.IdKey}";
                var command = new SqlCommand(deleteCommandText, connection)
                {
                    CommandType = CommandType.Text
                };
                command.Parameters.Add(new SqlParameter($"@{AzureDataLakeConstants.IdKey}", syncItem.EntityId));
                var rowsAffected = await command.ExecuteNonQueryAsync();
                if (rowsAffected != 1)
                {
                    throw new ApplicationException($"Rows affected for deletion is not 1, it is {rowsAffected}.");
                }
            }
            else
            {
                var insertOrUpdateSql = $"""
                        IF EXISTS (
                            SELECT 1 FROM [{tableName}] WITH (XLOCK, ROWLOCK)
                            WHERE {AzureDataLakeConstants.IdKey} = @{AzureDataLakeConstants.IdKey})  
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
                        """;
                var command = new SqlCommand(insertOrUpdateSql, connection)
                {
                    CommandType = CommandType.Text
                };
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

        private static List<string> GetPropertyKeysWithoutId(SyncItem syncItem)
        {
            return syncItem.Data.Keys.Except(new[] { AzureDataLakeConstants.IdKey }).OrderBy(key => key).ToList();
        }

        private static bool GetIsTableNotFoundException(SqlException ex)
        {
            return ex.Number == 208;
        }

        private static string GetCacheTableName(Guid streamId, bool isTestTable = false)
        {
            if (isTestTable)
            {
                return $"testConnection_{streamId}";
            }

            return CacheTableHelper.GetCacheTableName(streamId);
        }

        private static object GetDatabaseValue(object value)
        {
            if (value == null)
            {
                return DBNull.Value;
            }

            if (_dotNetToSqlTypeMap.ContainsKey(value.GetType()))
            {
                return value;
            }
            return JsonConvert.SerializeObject(value, _serializerSettings);
        }

        private async Task<SaveResult> WriteToOutputImmediately(
            IReadOnlyStreamModel streamModel,
            IReadOnlyConnectorEntityData connectorEntityData,
            AzureDataLakeConnectorJobData configurations,
            Dictionary<string, object> data)
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
                    var json = JsonConvert.SerializeObject(data, GetJsonSerializerSettings());

                    await _client.SaveData(configurations, json, filePathAndName, JsonMimeType);

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

        private static JsonSerializerSettings GetJsonSerializerSettings()
        {
            return new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None,
                Formatting = Formatting.Indented,
            };
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
                        return new ConnectionVerificationResult(false, $"Stream cache connection string must be valid when buffer is enabled.");
                    }

                    await VerifyTableOperations(jobData.StreamCacheConnectionString);
                }
                else
                {
                    if (!AzureDataLakeConstants.OutputFormats.Json.Equals(jobData.OutputFormat, StringComparison.OrdinalIgnoreCase))
                    {
                        return new ConnectionVerificationResult(false, $"Only JSON is supported when stream cache is disabled.");
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

        private async Task VerifyTableOperations(string connectionString)
        {
            var testStreamId = Guid.NewGuid();
            var testTableName = GetCacheTableName(testStreamId, true);

            var entityId = Guid.NewGuid();
            var data = new Dictionary<string, object>
            {
                [AzureDataLakeConstants.IdKey] = entityId,
                ["testColumn"] = 1234,
            };

            var dataValueTypes = new Dictionary<string, Type>
            {
                [AzureDataLakeConstants.IdKey] = typeof(Guid),
                ["testColumn"] = typeof(string),
            };

            try
            {
                using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                await using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync();
                var baseSyncItem = new SyncItem(testStreamId, entityId, VersionChangeType.Added, data, dataValueTypes);
                await EnsureCacheTableExists(
                        connection,
                        testTableName,
                        baseSyncItem);
                await VerifyOperation(connection, testTableName, baseSyncItem);
                await VerifyOperation(connection, testTableName, baseSyncItem with { ChangeType = VersionChangeType.Changed });
                await VerifyOperation(connection, testTableName, baseSyncItem with { ChangeType = VersionChangeType.Removed });
                await RenameCacheTableIfExists(connection, testTableName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to verify table operations.");
                throw;
            }
        }

        private async Task VerifyOperation(SqlConnection connection, string tableName, SyncItem syncItem)
        {
            try
            {
                await WriteToCacheTable(connection, syncItem, tableName);

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to perform operation '{ChangeType}' to the test table.", syncItem.ChangeType);
                throw;
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

            _client.SaveData(configuration, content, fileName, JsonMimeType).GetAwaiter().GetResult();
        }
        
        public override async Task CreateContainer(ExecutionContext executionContext, Guid connectorProviderDefinitionId, IReadOnlyCreateContainerModelV2 model)
        {
            await Task.CompletedTask;
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            await _buffer.Flush();
            await RenameCacheTableIfExists(executionContext, streamModel);
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

        private async Task RenameCacheTableIfExists(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
            var containerName = streamModel.ContainerName;

            var jobData = await AzureDataLakeConnectorJobData.Create(executionContext, providerDefinitionId, containerName);
            if (string.IsNullOrWhiteSpace(jobData.StreamCacheConnectionString))
            {
                _logger.LogDebug("Skipping renaming of cache table because stream cache connection string is null or whitespace.");
                return;
            }

            var tableName = GetCacheTableName(streamModel.Id);

            try
            {
                using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                await using var connection = new SqlConnection(jobData.StreamCacheConnectionString);
                await connection.OpenAsync();
                await RenameCacheTableIfExists(connection, tableName);
                transactionScope.Complete();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to delete table.");
            }
        }

        private static async Task RenameCacheTableIfExists(SqlConnection connection, string tableName)
        {
            var suffixDate = DateTime.UtcNow.ToString("yyyyMMddHHmmss");

            await renameTable($"{tableName}_History");
            await renameTable(tableName);

            async Task renameTable(string currentTableName)
            {
                var oldTableName = currentTableName;
                var newTableName = $"{currentTableName}_{suffixDate}";
                var renameTableSql = @$"
                IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @OldTableName AND TABLE_SCHEMA = @Schema)
                BEGIN
                    DECLARE @FullOldTableName SYSNAME = @Schema + N'.' + @OldTableName
                    EXEC sp_rename @FullOldTableName, @NewTableName;
                END

                WHILE EXISTS(
                    SELECT [CONSTRAINT_NAME] 
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
                    WHERE 
                        [TABLE_NAME] = @NewTableName 
                        AND 
                        NOT [CONSTRAINT_NAME] LIKE '%' + @ArchiveSuffix)
                BEGIN
                    DECLARE @ConstraintName SYSNAME;
                    SELECT TOP 1 @ConstraintName = [CONSTRAINT_NAME] 
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
                    WHERE 
                        [TABLE_NAME] = @NewTableName
                        AND 
                        NOT [CONSTRAINT_NAME] LIKE '%' + @ArchiveSuffix;

                    DECLARE @FullConstraintName SYSNAME = @Schema + '.' + @ConstraintName;
                    DECLARE @NewConstraintName SYSNAME = @ConstraintName + @archiveSuffix;
                    EXEC sp_rename @objname = @FullConstraintName, @newname = @NewConstraintName, @objtype = N'OBJECT';
                END";
                var schemaParameter = new SqlParameter("@Schema", SqlDbType.NVarChar) { Value = "dbo" };
                var oldTableNameParameter = new SqlParameter("@OldTableName", SqlDbType.NVarChar) { Value = oldTableName.ToString() };
                var newTableNameParameter = new SqlParameter("@NewTableName", SqlDbType.NVarChar) { Value = newTableName.ToString() };
                var archiveSuffixParameter = new SqlParameter("@ArchiveSuffix", SqlDbType.NVarChar) { Value = suffixDate };
                var parameters = new[] { schemaParameter, oldTableNameParameter, newTableNameParameter, archiveSuffixParameter };

                var command = new SqlCommand(renameTableSql, connection)
                {
                    CommandType = CommandType.Text
                };
                command.Parameters.AddRange(parameters);
                _ = await command.ExecuteNonQueryAsync();
            }
        }

        private record SyncItem(
            Guid StreamId,
            Guid EntityId,
            VersionChangeType ChangeType,
            IDictionary<string, object> Data,
            Dictionary<string, Type> DataValueTypes);
    }
}
