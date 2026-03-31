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

using Hangfire.Storage;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.FileStorage.Common.Connector
{
    public abstract partial class StorageConnectorBase : ConnectorBaseV2
    {
        protected static readonly ConnectionVerificationResult SuccessfulConnectionVerification = new (true);
        private const string JsonMimeType = "application/json";
        private const int TableCreationLockTimeoutInMilliseconds = 100;
        private static readonly char[] _invalidFileNameCharacters = ['/', '\\', '?', '%'];
        private static readonly string _invalidFileNameHasInvalidCharacters = $"File name contains invalid characters. It cannot have {string.Join(", ", _invalidFileNameCharacters.Select(c => $"'{c}'"))} characters";
        private const string InvalidFileNameStartsWithPeriodErrorMessage = "File name pattern cannot start with a period.";
        private readonly ILogger<StorageConnectorBase> _logger;
        private readonly ApplicationContext _applicationContext;
        private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
        private readonly IStorageFactory _storageFactory;
        private readonly PartitionedBuffer<Partition, string> _buffer;
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

        protected IStorageFactory StorageFactory => _storageFactory;

        protected StorageConnectorBase(
            ILogger<StorageConnectorBase> logger,
            ApplicationContext applicationContext,
            IStorageConfigurationConstants constants,
            IStorageFactory storageFactory,
            IDateTimeOffsetProvider dateTimeOffsetProvider)
            : base(constants.ProviderId, false)
        {
            _logger = logger;
            _applicationContext = applicationContext;
            _dateTimeOffsetProvider = dateTimeOffsetProvider;
            _storageFactory = storageFactory;

            var cacheRecordsThreshold = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheRecordsThresholdKeyName, constants.CacheRecordsThresholdDefaultValue);
            var backgroundFlushMaxIdleDefaultValue = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheSyncIntervalKeyName, constants.CacheSyncIntervalDefaultValue);
            var cacheStrategyValue = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheBufferStrategyKeyName, constants.CacheBufferStrategyDefaultValue);

            if (!Enum.TryParse(cacheStrategyValue, ignoreCase: true, out BufferStrategy cacheBufferStrategy))
            {
                logger.LogWarning("Invalid value for buffer {CacheBufferKeyName}. Using default {CacheBufferDefaultValue}", constants.CacheBufferStrategyKeyName, constants.CacheBufferStrategyDefaultValue);
                cacheBufferStrategy = Enum.Parse<BufferStrategy>(constants.CacheBufferStrategyDefaultValue);
            }

            _buffer = new PartitionedBuffer<Partition, string>(cacheRecordsThreshold,
                backgroundFlushMaxIdleDefaultValue, Flush, dateTimeOffsetProvider, cacheBufferStrategy);
        }

        ~StorageConnectorBase()
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
            var configuration = await _storageFactory.CreateStorageConfiguration(executionContext, providerDefinitionId, containerName);

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

            AddToData(StorageConfigurationConstants.IdKey, connectorEntityData.EntityId);
            AddToData("PersistHash", connectorEntityData.PersistInfo?.PersistHash);
            AddToData(StorageConfigurationConstants.PersistVersionKey, connectorEntityData.PersistInfo?.PersistVersion);
            AddToData("OriginEntityCode", connectorEntityData.OriginEntityCode?.ToString());
            AddToData("EntityType", connectorEntityData.EntityType?.ToString());
            AddToData("Codes", connectorEntityData.EntityCodes.SafeEnumerate().Select(code => code.ToString()));
            AddToData("ProviderDefinitionId", providerDefinitionId);
            AddToData("ContainerName", containerName);

            var now = _dateTimeOffsetProvider.GetCurrentUtcTime();

            if (!data.ContainsKey(StorageConfigurationConstants.TimestampKey))
            {
                AddToData(StorageConfigurationConstants.TimestampKey, now.ToString("O"));
            }

            if (!data.ContainsKey(StorageConfigurationConstants.EpochKey))
            {
                AddToData(StorageConfigurationConstants.EpochKey, now.ToUnixTimeMilliseconds());
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
                if (configuration.IsStreamCacheEnabled && streamModel.Mode == StreamMode.Sync)
                {
                    if (!data.ContainsKey(StorageConfigurationConstants.ChangeTypeKey))
                    {
                        AddToData(StorageConfigurationConstants.ChangeTypeKey, connectorEntityData.ChangeType.ToString());
                    }
                    return await WriteToCacheTable(streamModel, connectorEntityData, configuration, data, dataValueTypes);
                }
                else
                {
                    return await WriteToOutputImmediately(executionContext, streamModel, connectorEntityData, configuration, data);
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
                        [{StorageConfigurationConstants.IdKey}] UNIQUEIDENTIFIER NOT NULL,
                        {string.Join(string.Empty, propertiesColumns.Select(prop => $"{prop},\n    "))}
                        [ValidFrom] DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN,
                        [ValidTo] DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN,
                        PERIOD FOR SYSTEM_TIME(ValidFrom, ValidTo),
                        CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ([{StorageConfigurationConstants.IdKey}])
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
            IStorageConfiguration configurations,
            Dictionary<string, object> data,
            Dictionary<string, Type> dataValueTypes)
        {
            if (streamModel.Mode != StreamMode.Sync)
            {
                _logger.LogError($"Buffer mode is only supported with '{StreamMode.Sync}' mode.");
                return SaveResult.Failed;
            }

            var syncItem = new SyncItem(
                streamModel.Id,
                connectorEntityData.EntityId,
                connectorEntityData.PersistInfo?.PersistVersion,
                connectorEntityData.ChangeType,
                data,
                dataValueTypes);
            var tableName = GetCacheTableName(syncItem.StreamId);

            try
            {
                using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                await using var connection = new SqlConnection(configurations.StreamCacheConnectionString);
                await connection.OpenAsync();
                await WriteToCacheTable(connection, syncItem, tableName, useSoftDelete: true);
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
                    await WriteToCacheTable(connection, syncItem, tableName, useSoftDelete: true);
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
                TableCreationLockTimeoutInMilliseconds);
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
                // Prevent updates to removed records
                // But allow recreation of removed records (e.g: Unmerge Deduplication)
                var changeTypeConstraint = syncItem.ChangeType == VersionChangeType.Changed
                    ? $"AND [{StorageConfigurationConstants.ChangeTypeKey}] != '{VersionChangeType.Removed.ToString()}'"
                    : string.Empty;

                var insertOrUpdateSql = $"""
                        IF EXISTS (
                            SELECT 1 FROM [{tableName}] WITH (XLOCK, ROWLOCK)
                            WHERE
                                [{StorageConfigurationConstants.IdKey}] = @{StorageConfigurationConstants.IdKey})
                        BEGIN
                            UPDATE
                                [{tableName}]
                            SET
                                {string.Join(",\n        ", propertyKeys.Select((key, index) => $"[{key}] = @p{index}"))}
                            WHERE
                                [{StorageConfigurationConstants.IdKey}] = @{StorageConfigurationConstants.IdKey} AND
                                [{StorageConfigurationConstants.PersistVersionKey}] < @EntityPersionVersion
                              {changeTypeConstraint};
                        END
                        ELSE
                        BEGIN
                            INSERT INTO
                                [{tableName}]
                                ([{StorageConfigurationConstants.IdKey}]{string.Join(string.Empty, propertyKeys.Select(key => $", [{key}]"))})
                            VALUES(@{StorageConfigurationConstants.IdKey}{string.Join(string.Empty, propertyKeys.Select((_, index) => $", @p{index}"))})
                        END
                        """;
                var command = new SqlCommand(insertOrUpdateSql, connection)
                {
                    CommandType = CommandType.Text
                };
                command.Parameters.Add(new SqlParameter($"@EntityPersionVersion", syncItem.PersistVersion));
                command.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.IdKey}", syncItem.EntityId));

                for (var i = 0; i < propertyKeys.Count; i++)
                {
                    var key = propertyKeys[i];
                    command.Parameters.Add(new SqlParameter($"@p{i}", GetDatabaseValue(syncItem, key)));
                }

                var rowsAffected = await command.ExecuteNonQueryAsync();
                if (rowsAffected != 1)
                {
                    // check if row exists to determine if failure was due to update condition not being met or some other issue
                    if (rowsAffected == 0)
                    {
                        await VerifyRowUpToDate(connection, syncItem, tableName);
                        return;
                    }

                    throw new ApplicationException($"Rows affected for upsert of is not 1, it is {rowsAffected} for item {syncItem.EntityId}.");
                }
            }

            static async Task HardDeleteEntity(SqlConnection connection, SyncItem syncItem, string tableName)
            {
                var deleteCommandText = $"DELETE FROM [{tableName}] WHERE {StorageConfigurationConstants.IdKey} = @{StorageConfigurationConstants.IdKey}";
                var command = new SqlCommand(deleteCommandText, connection)
                {
                    CommandType = CommandType.Text
                };
                command.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.IdKey}", syncItem.EntityId));
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
                            WHERE [{StorageConfigurationConstants.IdKey}] = @{StorageConfigurationConstants.IdKey})
                        BEGIN
                            UPDATE [{tableName}]
                            SET
                                [{StorageConfigurationConstants.ChangeTypeKey}] = @{StorageConfigurationConstants.ChangeTypeKey},
                                [{StorageConfigurationConstants.TimestampKey}] = @{StorageConfigurationConstants.TimestampKey},
                                [{StorageConfigurationConstants.EpochKey}] = @{StorageConfigurationConstants.EpochKey}
                            WHERE [{StorageConfigurationConstants.IdKey}] = @{StorageConfigurationConstants.IdKey};
                        END
                        """;
                var command = new SqlCommand(updateSql, connection)
                {
                    CommandType = CommandType.Text
                };
                command.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.IdKey}", syncItem.EntityId));
                command.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.ChangeTypeKey}", syncItem.ChangeType.ToString()));
                command.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.TimestampKey}", syncItem.Data[StorageConfigurationConstants.TimestampKey]));
                command.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.EpochKey}", syncItem.Data[StorageConfigurationConstants.EpochKey]));

                var rowsAffected = await command.ExecuteNonQueryAsync();
                if (rowsAffected != 1)
                {
                    throw new ApplicationException($"Rows affected for soft deletion of is not 1, it is {rowsAffected}.");
                }
            }

            static async Task VerifyRowUpToDate(SqlConnection connection, SyncItem syncItem, string tableName)
            {
                var getVersionSql = $"""
                            SELECT
                                [{StorageConfigurationConstants.IdKey}],
                                [{StorageConfigurationConstants.PersistVersionKey}],
                                [{StorageConfigurationConstants.ChangeTypeKey}]
                            FROM
                                [{tableName}]
                            WITH
                                (XLOCK, ROWLOCK)
                            WHERE
                                [{StorageConfigurationConstants.IdKey}] = @{StorageConfigurationConstants.IdKey};
                            """;
                var getVersionCommand = new SqlCommand(getVersionSql, connection)
                {
                    CommandType = CommandType.Text
                };
                getVersionCommand.Parameters.Add(new SqlParameter($"@{StorageConfigurationConstants.IdKey}", syncItem.EntityId));
                await using var getVersionReader = await getVersionCommand.ExecuteReaderAsync();
                if (!await getVersionReader.ReadAsync())
                {
                    throw new ApplicationException($"No rows updated for upsert and entity could not be found.");
                }

                var changeType = getVersionReader.GetValue(StorageConfigurationConstants.ChangeTypeKey).ToString();
                var persistVersion = Convert.ToInt32(getVersionReader.GetValue(StorageConfigurationConstants.PersistVersionKey));

                if (!Enum.TryParse<VersionChangeType>(changeType, out var parsedChangedType))
                {
                    throw new ApplicationException($"Unable to parse change type '{changeType}' from the cache table item {syncItem.EntityId}.");
                }

                if (parsedChangedType != VersionChangeType.Removed &&
                    syncItem.PersistVersion != null &&
                    persistVersion < syncItem.PersistVersion)
                {
                    throw new ApplicationException($"Unable to update cache table item {syncItem.EntityId} from PersistVersion '{persistVersion}' to '{syncItem.PersistVersion}'.");
                }
            }
        }

        private static List<string> GetPropertyKeysWithoutId(SyncItem syncItem)
        {
            return syncItem.Data.Keys.Except(new[] { StorageConfigurationConstants.IdKey }).OrderBy(key => key).ToList();
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
            ExecutionContext executionContext,
            IReadOnlyStreamModel streamModel,
            IReadOnlyConnectorEntityData connectorEntityData,
            IStorageConfiguration configurations,
            Dictionary<string, object> data)
        {
            if (streamModel.Mode == StreamMode.Sync)
            {
                var filePathAndName = $"{connectorEntityData.EntityId.ToString().Substring(0, 2)}/{connectorEntityData.EntityId.ToString().Substring(2, 2)}/{connectorEntityData.EntityId}.json";

                var client = await _storageFactory.CreateStorageClient(executionContext, configurations);
                var baseDirectory = await client.GetBaseDirectoryPath();
                if (connectorEntityData.ChangeType == VersionChangeType.Removed)
                {
                    await client.DeleteFile(new (filePathAndName, baseDirectory));
                }
                else
                {
                    var json = JsonConvert.SerializeObject(data, _immediateOutputSerializerSettings);

                    await client.SaveData(new(filePathAndName, baseDirectory), json, JsonMimeType);

                }
            }
            else
            {
                data.Add("ChangeType", connectorEntityData.ChangeType.ToString());

                var json = JsonConvert.SerializeObject(data);

                await _buffer.Add(new (executionContext.Organization.Id, configurations), json);
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
                var configuration = await _storageFactory.CreateStorageConfiguration(executionContext, config.ToDictionary(config => config.Key, config => config.Value));
                return await VerifyConnectionInternal(executionContext, configuration);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error verifying connection");
                return new ConnectionVerificationResult(false, e.Message);
            }
        }

        protected virtual async Task<ConnectionVerificationResult> VerifyConnectionInternal(ExecutionContext executionContext, IStorageConfiguration configuration)
        {
            var verifyConnectionResult = await VerifyDataLakeConnection(executionContext, configuration);
            if (!verifyConnectionResult.Success)
            {
                return verifyConnectionResult;
            }

            if (configuration.IsStreamCacheEnabled)
            {
                if (string.IsNullOrWhiteSpace(configuration.StreamCacheConnectionString))
                {
                    return CreateFailedConnectionVerification("Stream cache connection string must be valid when buffer is enabled.");
                }

                await VerifyTableOperations(configuration.StreamCacheConnectionString);

                if (!StorageConfigurationConstants.OutputFormats.IsValid(configuration.OutputFormat))
                {
                    var supported = string.Join(',', StorageConfigurationConstants.OutputFormats.AllSupportedFormats);
                    var errorMessage = $"Format '{configuration.OutputFormat}' is not supported. Supported formats are {supported}.";
                    return CreateFailedConnectionVerification(errorMessage);
                }

                var cronOrScheduleName = configuration.GetCronOrScheduleName();
                if (string.IsNullOrWhiteSpace(cronOrScheduleName))
                {
                    return CreateFailedConnectionVerification("Schedule name cannot be empty.");
                }

                if (!CronSchedules.TryGetCronSchedule(cronOrScheduleName, out _))
                {
                    var supported = string.Join(',', CronSchedules.SupportedCronScheduleNames);
                    var errorMessage = $"Schedule '{configuration.Schedule}' with cron '{configuration.CustomCron}' is not supported. Supported schedules are {supported} and valid cron expression.";
                    return CreateFailedConnectionVerification(errorMessage);
                }

                if (!string.IsNullOrWhiteSpace(configuration.FileNamePattern))
                {
                    var trimmed = configuration.FileNamePattern.Trim();
                    if (trimmed.StartsWith("."))
                    {
                        return CreateFailedConnectionVerification(InvalidFileNameStartsWithPeriodErrorMessage);
                    }

                    if (trimmed.IndexOfAny(_invalidFileNameCharacters) != -1)
                    {
                        return CreateFailedConnectionVerification(_invalidFileNameHasInvalidCharacters);
                    }
                }
            }

            return SuccessfulConnectionVerification;
        }

        protected virtual async Task<ConnectionVerificationResult> VerifyDataLakeConnection(ExecutionContext executionContext, IStorageConfiguration jobData)
        {
            var client = await _storageFactory.CreateStorageClient(executionContext, jobData);
            await client.VerifyConnection();
            return SuccessfulConnectionVerification;
        }

        protected virtual ConnectionVerificationResult CreateFailedConnectionVerification(string message)
        {
            return new ConnectionVerificationResult(false, message);
        }

        private async Task VerifyTableOperations(string connectionString)
        {
            var testStreamId = Guid.NewGuid();
            var testTableName = GetCacheTableName(testStreamId, true);

            var entityId = Guid.NewGuid();
            var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
            var data = new Dictionary<string, object>
            {
                [StorageConfigurationConstants.IdKey] = entityId,
                [StorageConfigurationConstants.ChangeTypeKey] = VersionChangeType.Added.ToString(),
                [StorageConfigurationConstants.PersistVersionKey] = 1,
                [StorageConfigurationConstants.TimestampKey] = now,
                [StorageConfigurationConstants.EpochKey] = now.ToUnixTimeMilliseconds(),
                ["testColumn"] = 1234,
            };

            var dataValueTypes = new Dictionary<string, Type>
            {
                [StorageConfigurationConstants.IdKey] = typeof(Guid),
                [StorageConfigurationConstants.ChangeTypeKey] = typeof(string),
                [StorageConfigurationConstants.PersistVersionKey] = typeof(int),
                [StorageConfigurationConstants.TimestampKey] = typeof(DateTimeOffset),
                [StorageConfigurationConstants.EpochKey] = typeof(long),
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

                var baseSyncItem = new SyncItem(testStreamId, entityId, PersistVersion: 1, VersionChangeType.Added, data, dataValueTypes);
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
                var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
                syncItem.Data[StorageConfigurationConstants.ChangeTypeKey] = syncItem.ChangeType.ToString();
                syncItem.Data[StorageConfigurationConstants.PersistVersionKey] = syncItem.PersistVersion;
                syncItem.Data[StorageConfigurationConstants.TimestampKey] = now;
                syncItem.Data[StorageConfigurationConstants.EpochKey] = now.ToUnixTimeMilliseconds();
                await WriteToCacheTable(connection, syncItem, tableName, useSoftDelete: true);

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to perform operation '{ChangeType}' to the test table.", syncItem.ChangeType);
                throw;
            }
        }

        private async Task Flush(Partition partition, string[] entityData)
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

            var configuration = partition.JobData;
            var organizationId = partition.OrganizationId;
            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm-ss.fffffff");
            var fileName = $"{configuration.ContainerName}.{timestamp}.json";

            await using var executionContext = _applicationContext.CreateExecutionContext(organizationId);
            var client = await _storageFactory.CreateStorageClient(executionContext, configuration);
            var baseDirectory = await client.GetBaseDirectoryPath();
            await client.SaveData(new FilePath(fileName, baseDirectory), content, JsonMimeType);
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

            var configuration = await _storageFactory.CreateStorageConfiguration(executionContext, providerDefinitionId, "");
            var client = await _storageFactory.CreateStorageClient(executionContext, configuration);
            var baseDirectory = await client.GetBaseDirectoryPath();
            var files = await client.GetFilesInDirectory(baseDirectory);
            return files.Select(file => new StorageContainer()
            {
                Name = file.Name,
                FullyQualifiedName = file.FullyQualifiedName
            });
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

            var jobData = await _storageFactory.CreateStorageConfiguration(executionContext, providerDefinitionId, containerName);
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
            int? PersistVersion,
            VersionChangeType ChangeType,
            IDictionary<string, object> Data,
            Dictionary<string, Type> DataValueTypes);
        private record Partition(Guid OrganizationId, IStorageConfiguration JobData);
    }
}
