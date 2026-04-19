using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore;
using CluedIn.Core.DataStore.Entities;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common;

internal class StorageDataMigrator : DataMigrator
{
    private readonly DbContextOptions<CluedInEntities> _cluedInEntitiesDbContextOptions;
    protected readonly IStorageConfigurationConstants _dataLakeConstants;
    protected readonly IStorageFactory _dataLakeJobDataFactory;

    public StorageDataMigrator(
        ILogger logger,
        ApplicationContext applicationContext,
        DbContextOptions<CluedInEntities> cluedInEntitiesDbContextOptions,
        string componentName,
        IStorageConfigurationConstants constants,
        IStorageFactory dataLakeJobDataFactory) : base (logger, applicationContext, cluedInEntitiesDbContextOptions, componentName)
    {
        _cluedInEntitiesDbContextOptions = cluedInEntitiesDbContextOptions;
        _dataLakeConstants = constants ?? throw new ArgumentNullException(nameof(constants));
        _dataLakeJobDataFactory = dataLakeJobDataFactory ?? throw new ArgumentNullException(nameof(dataLakeJobDataFactory));
    }

    protected override async Task RunMigrations()
    {
        await MigrateAccountId();
        await MigrateSoftDelete();
    }

    protected virtual async Task MigrateAccountId()
    {
        await MigrateForOrganizations("EmptyAccountId", migrateAccountIdForProviderDefinition);
        async Task migrateAccountIdForProviderDefinition(ExecutionContext context, string componentMigrationName)
        {
            var organizationId = context.Organization.Id;
            var store = context.Organization.DataStores.GetDataStore<ProviderDefinition>();
            var definitions = await store.SelectAsync(
                context,
                definition => definition.OrganizationId == context.Organization.Id
                                && definition.ProviderId == _dataLakeConstants.ProviderId);
            foreach (var definition in definitions)
            {
                if (definition.AccountId != string.Empty)
                {
                    _logger.LogDebug("Skipping provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                        componentMigrationName,
                        organizationId,
                        definition.Id);
                    continue;
                }

                _logger.LogInformation("Begin provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                    componentMigrationName,
                    organizationId,
                    definition.Id);
                definition.AccountId = AccountIdHelper.Generate(_dataLakeConstants.ProviderId, definition.Id);
                await store.UpdateAsync(context, definition);
                _logger.LogInformation("End provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                    componentMigrationName,
                    organizationId,
                    definition.Id);
            }
        }
    }

    protected virtual async Task MigrateSoftDelete()
    {
        await MigrateForOrganizations("SoftDeleteDefault", MigrateSoftDeleteForProviderDefinition);
        async Task MigrateSoftDeleteForProviderDefinition(ExecutionContext context, string componentMigrationName)
        {
            await using var dbContext = new CluedInEntities(_cluedInEntitiesDbContextOptions);
            var organizationId = context.Organization.Id;
            var store = context.Organization.DataStores.GetDataStore<ProviderDefinition>();
            var definitions = await store.SelectAsync(
                context,
                definition => definition.OrganizationId == context.Organization.Id
                                && definition.ProviderId == _dataLakeConstants.ProviderId);

            var connectionStrings = context.ApplicationContext.System.ConnectionStrings;
            var connectionStringKey = StorageConfigurationConstants.StreamCacheConnectionStringKey;
            var configurationKey = StorageConfigurationConstants.StreamCacheConnectionString;
            if (!connectionStrings.ConnectionStringExists(connectionStringKey))
            {
                throw new InvalidOperationException("Stream cache connection string is not found.");
            }

            await using var connection = new SqlConnection(connectionStrings.GetConnectionString(connectionStringKey));
            await connection.OpenAsync();

            var configurationRepository = context.ApplicationContext.Container.Resolve<IConfigurationRepository>();
            foreach (var definition in definitions)
            {
                _logger.LogInformation("Begin provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                    componentMigrationName,
                    organizationId,
                    definition.Id);

                var streams = await dbContext.Streams
                    .Where(stream => stream.OrganizationId == organizationId &&
                            stream.ConnectorProviderDefinitionId == definition.Id)
                    .ToListAsync();

                var configuration = configurationRepository.GetConfigurationById(context, definition.Id);
                if (!IsStreamCacheEnabled(configuration))
                {
                    _logger.LogInformation("Stream cache is not enabled for ProviderDefinition '{ProviderDefinitionId}'. Skipping stream migration for soft delete.",
                        definition.Id);
                    continue;
                }

                foreach (var stream in streams)
                {
                    if (stream.Mode != StreamMode.Sync)
                    {
                        _logger.LogInformation("Skipping stream migration for stream '{StreamId}' with mode '{StreamMode}'.",
                            stream.Id,
                            stream.Mode);
                        continue;
                    }

                    if (stream.Status == StreamStatus.Stopped || stream.Status == StreamStatus.New)
                    {
                        _logger.LogInformation("Skipping stream migration for stream '{StreamId}' with status '{StreamStatus}'.",
                            stream.Id,
                            stream.Status);
                        continue;
                    }

                    var tableName = CacheTableHelper.GetCacheTableName(stream.Id);
                    _logger.LogInformation("Begin migrating export table '{tableName}' for stream '{StreamId}' for soft delete migration.",
                        tableName,
                        stream.Id);
                    try
                    {
                        var alterTableSql = $"ALTER TABLE [{tableName}] ADD [{StorageConfigurationConstants.ChangeTypeKey}] [nvarchar](MAX) NULL";
                        var command = new SqlCommand(alterTableSql, connection)
                        {
                            CommandType = CommandType.Text
                        };

                        _ = await command.ExecuteNonQueryAsync();
                    }
                    catch (SqlException writeDataException) when (writeDataException.IsCannotFindTableException())
                    {
                        _logger.LogWarning(writeDataException, "Cache table '{TableName}' is not found for stream '{StreamId}'. It is possible that the stream cache table has not been created yet. Skipping this stream for soft delete migration.",
                            tableName,
                            stream.Id);
                    }
                    catch (SqlException writeDataException) when (writeDataException.IsColumnAlreadyExistsException())
                    {
                        _logger.LogWarning(writeDataException, "Column '{ColumnName}' already exists in cache table '{TableName}' for stream '{StreamId}'. It is possible that this stream has already been migrated for soft delete. Skipping this stream for soft delete migration.",
                            StorageConfigurationConstants.ChangeTypeKey,
                            tableName,
                            stream.Id);
                    }
                    _logger.LogInformation("End migrating export table '{tableName}' for stream '{StreamId}' for soft delete migration.",
                        tableName,
                        stream.Id);
                }
                _logger.LogInformation("End provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                    componentMigrationName,
                    organizationId,
                    definition.Id);
            }
        }
    }
    protected bool IsStreamCacheEnabled(IDictionary<string, object> configuration)
    {
        if (configuration.TryGetValue(StorageConfigurationConstants.IsStreamCacheEnabled, out var value))
        {
            var casted = value as bool?;
            return casted is true;
        }

        return false;
    }
}
