using System;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore.Entities;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common;

internal class StorageDataMigrator : DataMigrator
{
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
        _dataLakeConstants = constants ?? throw new ArgumentNullException(nameof(constants));
        _dataLakeJobDataFactory = dataLakeJobDataFactory ?? throw new ArgumentNullException(nameof(dataLakeJobDataFactory));
    }

    protected override async Task RunMigrations()
    {
        await MigrateAccountId();
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
}
