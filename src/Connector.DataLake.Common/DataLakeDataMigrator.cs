using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore.Entities;
using CluedIn.Core.Streams;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common;

internal class DataLakeDataMigrator : DataMigrator
{
    protected readonly IDataLakeConstants _dataLakeConstants;
    protected readonly IDataLakeJobDataFactory _dataLakeJobDataFactory;
    private const int StreamsPerPage = 100;

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
            var organizationId = context.Organization.Id;
            var store = context.Organization.DataStores.GetDataStore<ProviderDefinition>();
            var definitions = await store.SelectAsync(
                context,
                definition => definition.OrganizationId == context.Organization.Id
                                && definition.ProviderId == _dataLakeConstants.ProviderId);
            foreach (var definition in definitions)
            {
                _logger.LogInformation("Begin provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                    componentMigrationName,
                    organizationId,
                    definition.Id);
                var streamRepository = _applicationContext.Container.Resolve<IStreamRepository>();
                var streamsCount = await streamRepository.GetOrganizationStreamsCount(context, filterConnectorProviderDefinitionId: definition.Id);
                var streamsPerPage = StreamsPerPage;
                var totalPages = (streamsCount + streamsPerPage - 1) / streamsPerPage;

                for (var i = 0; i < totalPages; ++i)
                {
                    var streams = await streamRepository.GetOrganizationStreams(context, i, streamsPerPage, filterConnectorProviderDefinitionId: definition.Id);
                    foreach (var stream in streams)
                    {
                        var connectorProperties = stream.ConnectorProperties == null ? new Dictionary<string, object>() : new Dictionary<string, object>(stream.ConnectorProperties);
                        connectorProperties.TryAdd(
                            DataLakeConstants.IsSoftDelete,
                            false);

                        await streamRepository.UpdateStream(context, stream.Id, stream, context.Organization.Id, stream.ModifiedBy);
                    }
                }

                _logger.LogInformation("End provider definition migration: '{MigrationName}' for organization '{OrganizationId}' and ProviderDefinition '{ProviderDefinitionId}'.",
                    componentMigrationName,
                    organizationId,
                    definition.Id);
            }
        }
    }
}
