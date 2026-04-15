using System;
using System.Reflection;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore;
using CluedIn.Core.DataStore.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common;

internal class DataLakeDataMigrator : DataMigrator
{
    protected readonly IDataLakeConstants _dataLakeConstants;
    protected readonly IDataLakeJobDataFactory _dataLakeJobDataFactory;

    public DataLakeDataMigrator(
        ILogger logger,
        ApplicationContext applicationContext,
        DbContextOptions<CluedInEntities> cluedInEntitiesDbContextOptions,
        string componentName,
        IDataLakeConstants constants,
        IDataLakeJobDataFactory dataLakeJobDataFactory) : base (logger, applicationContext, cluedInEntitiesDbContextOptions, componentName)
    {
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
            var invalidCacheMethod = typeof(AuthenticationDetailsHelper).GetMethod("InvalidateCacheAsync", [typeof(ApplicationContext), typeof(Guid)]);
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
                var configurationRepository = context.ApplicationContext.Container.Resolve<IConfigurationRepository>();
                var configuration = configurationRepository.GetConfigurationById(context, definition.Id);
                if (configuration == null)
                {
                    _logger.LogWarning("Configuration not found for ProviderDefinition '{ProviderDefinitionId}' during migration: '{MigrationName}' for organization '{OrganizationId}'. Skipping.",
                        definition.Id,
                        componentMigrationName,
                        organizationId);
                    continue;
                }
                configuration[nameof(DataLakeConstants.IsSoftDelete)] = false;
                configurationRepository.UpdateConfiguration(context, definition.Id, configuration);
                if (invalidCacheMethod == null)
                {
                    _logger.LogWarning("InvalidateCacheAsync method not found on AuthenticationDetailsHelper. Cache will not be cleared for ProviderDefinition '{ProviderDefinitionId}' during migration.",
                        definition.Id);
                }
                ClearConfigurationCache(context, definition.Id, invalidCacheMethod);
                _logger.LogInformation("End provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                    componentMigrationName,
                    organizationId,
                    definition.Id);
            }
        }
    }

    private async Task ClearConfigurationCache(
        ExecutionContext context,
        Guid definitionId,
        MethodInfo invalidCacheMethod)
    {
        var returnValue = (Task)invalidCacheMethod.Invoke(null, [context.ApplicationContext, definitionId]);
        if (returnValue != null)
        {
            await returnValue;
        }
    }
}
