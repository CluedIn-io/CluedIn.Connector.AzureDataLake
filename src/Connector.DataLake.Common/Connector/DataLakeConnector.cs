using System;
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

namespace CluedIn.Connector.DataLake.Common.Connector
{
    public abstract class DataLakeConnector : ConnectorBaseV2
    {
        private const string JsonMimeType = "application/json";
        private const int TableCreationLockTimeoutInMillliseconds = 100;
        private readonly ILogger<DataLakeConnector> _logger;
        private readonly IDataLakeClient _client;
        private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
        private readonly IDataLakeJobDataFactory _dataLakeJobDataFactory;
        private readonly PartitionedBuffer<IDataLakeJobData, string> _buffer;
        private static readonly JsonSerializerSettings _immediateOutputSerializerSettings = GetJsonSerializerSettings(Formatting.Indented);
        private static readonly JsonSerializerSettings _cacheTableSerializerSettings = GetJsonSerializerSettings(Formatting.None);

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

        protected IDataLakeJobDataFactory DataLakeJobDataFactory => _dataLakeJobDataFactory;

        protected IDataLakeClient Client => _client;

        protected DataLakeConnector(
            ILogger<DataLakeConnector> logger,
            IDataLakeClient client,
            IDataLakeConstants constants,
            IDataLakeJobDataFactory dataLakeJobDataFactory,
            IDateTimeOffsetProvider dateTimeOffsetProvider)
            : base(constants.ProviderId, false)
        {
            _logger = logger;
            _client = client;
            _dateTimeOffsetProvider = dateTimeOffsetProvider;
            _dataLakeJobDataFactory = dataLakeJobDataFactory;

            var cacheRecordsThreshold = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheRecordsThresholdKeyName, constants.CacheRecordsThresholdDefaultValue);
            var backgroundFlushMaxIdleDefaultValue = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheSyncIntervalKeyName, constants.CacheSyncIntervalDefaultValue);

            _buffer = new PartitionedBuffer<IDataLakeJobData, string>(cacheRecordsThreshold,
                backgroundFlushMaxIdleDefaultValue, Flush);
        }

        ~DataLakeConnector()
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
            var jobData = await _dataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName);

            // matching output format of previous version of the connector
            var data = connectorEntityData.Properties.ToDictionary(property => property.Name, property => property.Value);
            var dataValueTypes = new Dictionary<string, Type>();
            foreach(var property in connectorEntityData.Properties)
            {
                var type = property.GetDataType();
                type = RemoveNullableType(type);

                dataValueTypes.Add(property.Name, type);
            }

            void AddToData<T>(string key, T value)
            {
                data.Add(key, value);
                dataValueTypes.Add(key, RemoveNullableType(typeof(T)));
            }

            AddToData(DataLakeConstants.IdKey, connectorEntityData.EntityId);
            AddToData("PersistHash", connectorEntityData.PersistInfo?.PersistHash);
            AddToData("PersistVersion", connectorEntityData.PersistInfo?.PersistVersion);
            AddToData("OriginEntityCode", connectorEntityData.OriginEntityCode?.ToString());
            AddToData("EntityType", connectorEntityData.EntityType?.ToString());
            AddToData("Codes", connectorEntityData.EntityCodes.SafeEnumerate().Select(code => code.ToString()));
            AddToData("ProviderDefinitionId", providerDefinitionId);
            AddToData("ContainerName", containerName);

            var now = _dateTimeOffsetProvider.GetCurrentUtcTime();

            if (!data.ContainsKey("Timestamp"))
            {
                AddToData("Timestamp", now.ToString("O"));
            }

            if (!data.ContainsKey("Epoch"))
            {
                AddToData("Epoch", now.ToUnixTimeMilliseconds());
            }

            if (jobData.IsDeltaMode && !data.ContainsKey(DataLakeConstants.ChangeTypeKey))
            {
                AddToData(DataLakeConstants.ChangeTypeKey, connectorEntityData.ChangeType.ToString());
            }

            // end match previous version of the connector
            if (streamModel.ExportOutgoingEdges)
            {
                AddToData("OutgoingEdges", connectorEntityData.OutgoingEdges.SafeEnumerate());
            }
            if (streamModel.ExportIncomingEdges)
            {
                AddToData("IncomingEdges", connectorEntityData.IncomingEdges.SafeEnumerate());
            }

            try
            {
                if (jobData.IsStreamCacheEnabled && streamModel.Mode == StreamMode.Sync)
                {
                    return await WriteToCacheTable(streamModel, connectorEntityData, jobData, data, dataValueTypes);
                }
                else
                {
                    return await WriteToOutputImmediately(streamModel, connectorEntityData, jobData, data);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception thrown. Returning SaveResult.ReQueue");
                return SaveResult.ReQueue;
            }
        }

        private static Type RemoveNullableType(Type type)
        {
            var nullableUnderlyingType = Nullable.GetUnderlyingType(type);
            if (nullableUnderlyingType != null)
            {
                type = nullableUnderlyingType;
            }

            return type;
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
                BEGIN
                    CREATE TABLE [{tableName}] (
                        {DataLakeConstants.IdKey} UNIQUEIDENTIFIER NOT NULL,
                        {string.Join(string.Empty, propertiesColumns.Select(prop => $"{prop},\n    "))}
                        [ValidFrom] DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN,
                        [ValidTo] DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN,
                        PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo),
                        CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ({DataLakeConstants.IdKey})
                    ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.[{tableName}_History]));
                    CREATE INDEX [ValidFromValidTo] ON [{tableName}] ([ValidFrom], [ValidTo]);
                END
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
            IDataLakeJobData configurations,
            Dictionary<string, object> data,
            Dictionary<string, Type> dataValueTypes)
        {
            if (streamModel.Mode != StreamMode.Sync)
            {
                _logger.LogError($"Buffer mode is only supported with '{StreamMode.Sync}' mode.");
                return SaveResult.Failed;
            }

            var syncItem = new SyncItem(streamModel.Id, connectorEntityData.EntityId, connectorEntityData.ChangeType, data, dataValueTypes);
            var tableName = GetCacheTableName(syncItem.StreamId);

            try
            {
                using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                await using var connection = new SqlConnection(configurations.StreamCacheConnectionString);
                await connection.OpenAsync();
                await WriteToCacheTable(connection, syncItem, tableName, useSoftDelete: configurations.IsDeltaMode);
                transactionScope.Complete();
            }
            catch (SqlException writeDataException) when (writeDataException.IsTableNotFoundException())
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
                    await WriteToCacheTable(connection, syncItem, tableName, useSoftDelete: configurations.IsDeltaMode);
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
            var typeName = GetType().Name;
            return DistributedLockHelper.TryAcquireExclusiveLock(
                connection,
                $"{typeName}_{tableName}",
                TableCreationLockTimeoutInMillliseconds);
        }

        private async Task WriteToCacheTable(
            SqlConnection connection,
            SyncItem syncItem,
            string tableName,
            bool useSoftDelete)
        {
            var propertyKeys = GetPropertyKeysWithoutId(syncItem);
            if (syncItem.ChangeType == VersionChangeType.Removed)
            {
                if (useSoftDelete)
                {
                    await SoftDeleteEntity(connection, syncItem, tableName);
                }
                else
                {
                    await HardDeleteEntity(connection, syncItem, tableName);
                }
            }
            else
            {
                var insertOrUpdateSql = $"""
                        IF EXISTS (
                            SELECT 1 FROM [{tableName}] WITH (XLOCK, ROWLOCK)
                            WHERE {DataLakeConstants.IdKey} = @{DataLakeConstants.IdKey})
                        BEGIN
                            UPDATE [{tableName}]
                            SET
                                {string.Join(",\n        ", propertyKeys.Select((key, index) => $"[{key}] = @p{index}"))}
                            WHERE {DataLakeConstants.IdKey} = @{DataLakeConstants.IdKey};
                        END
                        ELSE
                        BEGIN
                            INSERT INTO [{tableName}] ({DataLakeConstants.IdKey}{string.Join(string.Empty, propertyKeys.Select(key => $", [{key}]"))})
                            VALUES(@{DataLakeConstants.IdKey}{string.Join(string.Empty, propertyKeys.Select((_, index) => $", @p{index}"))})
                        END
                        """;
                var command = new SqlCommand(insertOrUpdateSql, connection)
                {
                    CommandType = CommandType.Text
                };
                command.Parameters.Add(new SqlParameter($"@{DataLakeConstants.IdKey}", syncItem.EntityId));

                for (var i = 0; i < propertyKeys.Count; i++)
                {
                    var key = propertyKeys[i];
                    command.Parameters.Add(new SqlParameter($"@p{i}", GetDatabaseValue(syncItem, key)));
                }

                var rowsAffected = await command.ExecuteNonQueryAsync();
                if (rowsAffected != 1)
                {
                    throw new ApplicationException($"Rows affected for insertion of is not 1, it is {rowsAffected}.");
                }
            }

            static async Task HardDeleteEntity(SqlConnection connection, SyncItem syncItem, string tableName)
            {
                var deleteCommandText = $"DELETE FROM [{tableName}] WHERE {DataLakeConstants.IdKey} = @{DataLakeConstants.IdKey}";
                var command = new SqlCommand(deleteCommandText, connection)
                {
                    CommandType = CommandType.Text
                };
                command.Parameters.Add(new SqlParameter($"@{DataLakeConstants.IdKey}", syncItem.EntityId));
                var rowsAffected = await command.ExecuteNonQueryAsync();
                if (rowsAffected != 1)
                {
                    throw new ApplicationException($"Rows affected for hard deletion is not 1, it is {rowsAffected}.");
                }
            }

            static async Task SoftDeleteEntity(SqlConnection connection, SyncItem syncItem, string tableName)
            {
                var updateSql = $"""
                        IF EXISTS (
                            SELECT 1 FROM [{tableName}] WITH (XLOCK, ROWLOCK)
                            WHERE {DataLakeConstants.IdKey} = @{DataLakeConstants.IdKey})
                        BEGIN
                            UPDATE [{tableName}]
                            SET
                                [{DataLakeConstants.ChangeTypeKey}] = @ChangeType,
                                [Timestamp] = @Timestamp,
                                [Epoch] = @Epoch
                            WHERE {DataLakeConstants.IdKey} = @{DataLakeConstants.IdKey};
                        END
                        """;
                var command = new SqlCommand(updateSql, connection)
                {
                    CommandType = CommandType.Text
                };
                command.Parameters.Add(new SqlParameter($"@{DataLakeConstants.IdKey}", syncItem.EntityId));
                command.Parameters.Add(new SqlParameter($"@ChangeType", syncItem.ChangeType.ToString()));
                command.Parameters.Add(new SqlParameter($"@Timestamp", syncItem.Data["Timestamp"]));
                command.Parameters.Add(new SqlParameter($"@Epoch", syncItem.Data["Epoch"]));

                var rowsAffected = await command.ExecuteNonQueryAsync();
                if (rowsAffected != 1)
                {
                    throw new ApplicationException($"Rows affected for soft deletion of is not 1, it is {rowsAffected}.");
                }
            }
        }

        private static List<string> GetPropertyKeysWithoutId(SyncItem syncItem)
        {
            return syncItem.Data.Keys.Except(new[] { DataLakeConstants.IdKey }).OrderBy(key => key).ToList();
        }

        private static string GetCacheTableName(Guid streamId, bool isTestTable = false)
        {
            if (isTestTable)
            {
                return $"testConnection_{streamId}";
            }

            return CacheTableHelper.GetCacheTableName(streamId);
        }

        private static object GetDatabaseValue(SyncItem syncItem, string key)
        {
            var value = syncItem.Data[key];
            var dataValueType = syncItem.DataValueTypes[key];
            if (value == null)
            {
                return DBNull.Value;
            }

            // Bug with CluedIn where a DateTime vocab key is sent as string to bus but deserialized as DateTime
            // In CluedIn > 4.3.0, it's deserialized as DateTimeOffset instead of DateTime
            if (dataValueType == typeof(string) && (value is DateTime || value is DateTimeOffset))
            {
                var serialized = JsonConvert.SerializeObject(value, _cacheTableSerializerSettings);
                return serialized[1..^1];
            }


            if (_dotNetToSqlTypeMap.ContainsKey(value.GetType()))
            {
                return value;
            }

            return JsonConvert.SerializeObject(value, _cacheTableSerializerSettings);
        }

        private async Task<SaveResult> WriteToOutputImmediately(
            IReadOnlyStreamModel streamModel,
            IReadOnlyConnectorEntityData connectorEntityData,
            IDataLakeJobData configurations,
            Dictionary<string, object> data)
        {
            if (streamModel.Mode == StreamMode.Sync)
            {
                var filePathAndName = $"{connectorEntityData.EntityId.ToString().Substring(0, 2)}/{connectorEntityData.EntityId.ToString().Substring(2, 2)}/{connectorEntityData.EntityId}.json";

                if (connectorEntityData.ChangeType == VersionChangeType.Removed)
                {
                    await Client.DeleteFile(configurations, filePathAndName);
                }
                else
                {
                    var json = JsonConvert.SerializeObject(data, _immediateOutputSerializerSettings);

                    await Client.SaveData(configurations, json, filePathAndName, JsonMimeType);

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

        private static JsonSerializerSettings GetJsonSerializerSettings(Formatting formatting)
        {
            return new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None,
                Formatting = formatting,
                DateParseHandling = DateParseHandling.None,
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
                var jobData = await _dataLakeJobDataFactory.GetConfiguration(executionContext, config.ToDictionary(config => config.Key, config => config.Value));
                return await VerifyConnectionInternal(executionContext, jobData);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error verifying connection");
                return new ConnectionVerificationResult(false, e.Message);
            }
        }

        protected virtual async Task<ConnectionVerificationResult> VerifyConnectionInternal(ExecutionContext executionContext, IDataLakeJobData jobData)
        {
            if (!await VerifyDataLakeConnection(jobData))
            {
                return new ConnectionVerificationResult(false, "Data Lake connection cannot be established.");
            }

            if (jobData.IsStreamCacheEnabled)
            {
                if (string.IsNullOrWhiteSpace(jobData.StreamCacheConnectionString))
                {
                    return new ConnectionVerificationResult(false, $"Stream cache connection string must be valid when buffer is enabled.");
                }

                await VerifyTableOperations(jobData.StreamCacheConnectionString);

                if (!DataLakeConstants.OutputFormats.IsValid(jobData.OutputFormat))
                {
                    var supported = string.Join(',', DataLakeConstants.OutputFormats.AllSupportedFormats);
                    var errorMessage = $"Format '{jobData.OutputFormat}' is not supported. Supported formats are {supported}.";
                    return new ConnectionVerificationResult(false, errorMessage);
                }

                var cronOrScheduleName = jobData.GetCronOrScheduleName();
                if (string.IsNullOrWhiteSpace(cronOrScheduleName))
                {
                    return new ConnectionVerificationResult(false, "Schedule name cannot be empty.");
                }

                if (!CronSchedules.TryGetCronSchedule(cronOrScheduleName, out _))
                {
                    var supported = string.Join(',', CronSchedules.SupportedCronScheduleNames);
                    var errorMessage = $"Schedule '{jobData.Schedule}' with cron '{jobData.CustomCron}' is not supported. Supported schedules are {supported} and valid cron expression.";
                    return new ConnectionVerificationResult(false, errorMessage);
                }

                if (!string.IsNullOrWhiteSpace(jobData.FileNamePattern))
                {
                    var trimmed = jobData.FileNamePattern.Trim();
                    var invalidCharacters = new[] { '/', '\\', '?', '%' };
                    if (trimmed.StartsWith("."))
                    {
                        return new ConnectionVerificationResult(false, "File name pattern cannot start with a period.");
                    }
                    else if (trimmed.IndexOfAny(invalidCharacters) != -1)
                    {
                        return new ConnectionVerificationResult(false, "File name contains invalid characters.");
                    }
                }
            }

            return new ConnectionVerificationResult(true);
        }

        protected virtual async Task<bool> VerifyDataLakeConnection(IDataLakeJobData jobData)
        {
            await Client.EnsureDataLakeDirectoryExist(jobData);
            return true;
        }

        private async Task VerifyTableOperations(string connectionString)
        {
            var testStreamId = Guid.NewGuid();
            var testTableName = GetCacheTableName(testStreamId, true);

            var entityId = Guid.NewGuid();
            var data = new Dictionary<string, object>
            {
                [DataLakeConstants.IdKey] = entityId,
                ["testColumn"] = 1234,
            };

            var dataValueTypes = new Dictionary<string, Type>
            {
                [DataLakeConstants.IdKey] = typeof(Guid),
                ["testColumn"] = typeof(string),
            };

            try
            {
                using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                await using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync();

                if (!await DistributedLockHelper.TryAcquireExclusiveLock(connection, nameof(VerifyConnection), -1))
                {
                    throw new ApplicationException("Failed to acquire lock for verifying connection.");
                }

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
                await WriteToCacheTable(connection, syncItem, tableName, useSoftDelete: false);

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to perform operation '{ChangeType}' to the test table.", syncItem.ChangeType);
                throw;
            }
        }

        private void Flush(IDataLakeJobData configuration, string[] entityData)
        {
            if (entityData == null)
            {
                return;
            }

            if (entityData.Length == 0)
            {
                return;
            }

            var content = JsonConvert.SerializeObject(
                entityData.Select(x => (JObject)JsonConvert.DeserializeObject(x, _immediateOutputSerializerSettings)).ToArray(),
                _immediateOutputSerializerSettings);

            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm-ss.fffffff");
            var fileName = $"{configuration.ContainerName}.{timestamp}.json";

            Client.SaveData(configuration, content, fileName, JsonMimeType).GetAwaiter().GetResult();
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

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext,
            Guid providerDefinitionId)
        {
            _logger.LogInformation($"DataLakeConnector.GetContainers: entry");

            var jobData = await _dataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, "");

            return await _client.GetFilesInDirectory(jobData);
        }

        public override Task EmptyContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            _logger.LogInformation($"DataLakeConnector.EmptyContainer: entry");

            throw new NotImplementedException(nameof(EmptyContainer));
        }

        public override Task RenameContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, string oldContainerName)
        {
            _logger.LogInformation($"DataLakeConnector.RenameContainer: entry");

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
            _logger.LogInformation($"DataLakeConnector.RemoveContainer: entry");

            throw new NotImplementedException(nameof(RemoveContainer));
        }

        private async Task RenameCacheTableIfExists(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
            var containerName = streamModel.ContainerName;

            var jobData = await _dataLakeJobDataFactory.GetConfiguration(executionContext, providerDefinitionId, containerName);
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
