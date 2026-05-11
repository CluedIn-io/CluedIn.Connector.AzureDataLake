using System;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore.Entities;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake;

internal class AzureDataLakeDataMigrator : DataLakeDataMigrator
{
    public AzureDataLakeDataMigrator(
        ILogger logger,
        ApplicationContext applicationContext,
        DbContextOptions<CluedInEntities> cluedInEntitiesDbContextOptions,
        string componentName,
        IDataLakeConstants constants,
        IDataLakeJobDataFactory dataLakeJobDataFactory) : base(logger, applicationContext, cluedInEntitiesDbContextOptions, componentName, constants, dataLakeJobDataFactory)
    {
    }

    protected override async Task RunMigrations()
    {
        await MigrateMode();
        await base.RunMigrations();
    }

    private async Task MigrateMode()
    {
        // Set existing streams to EventMode
        try
        {
            var upgradeSettingKey = "ADl Mode Migration";

            var dbContext = new CluedInEntities(_applicationContext.Container.Resolve<DbContextOptions<CluedInEntities>>());
            var modeMigrationSetting = dbContext.Settings.FirstOrDefault(s =>
                s.OrganizationId == Guid.Empty && s.Key == upgradeSettingKey);
            if (modeMigrationSetting != null)
            {
                return;
            }

            var startedAt = DateTime.Now;

            IStreamRepository streamRepository = null;
            while (streamRepository == null)
            {
                if (DateTime.Now.Subtract(startedAt).TotalMinutes > 10)
                {
                    _logger.LogWarning($"Timeout resolving {nameof(IStreamRepository)}");
                    return;
                }

                try
                {
                    streamRepository = _applicationContext.Container.Resolve<IStreamRepository>();
                }
                catch
                {
                    await Task.Delay(1000);
                }
            }
            var orgDataStore = _applicationContext.System.Organization.DataStores.GetDataStore<OrganizationProfile>();
            var organizationProfiles = await orgDataStore.SelectAsync(_applicationContext.System.CreateExecutionContext(), _ => true);
            foreach (var organizationProfile in organizationProfiles)
            {
                var executionContext = _applicationContext.CreateExecutionContext(organizationProfile.Id);
                var streams = await streamRepository.GetAllStreams(executionContext).ToList();


                foreach (var provider in executionContext.Organization.Providers.AllProviderDefinitions.Where(x =>
                             x.ProviderId == _dataLakeConstants.ProviderId))
                {
                    foreach (var stream in streams.Where(s => s.ConnectorProviderDefinitionId == provider.Id))
                    {
                        if (stream.Mode != StreamMode.EventStream)
                        {

                            var model = new SetupConnectorModel
                            {
                                ConnectorProviderDefinitionId = provider.Id,
                                Mode = StreamMode.EventStream,
                                ContainerName = stream.ContainerName,
                                DataTypes =
                                    (await streamRepository.GetStreamMappings(executionContext, stream.Id))
                                    .Select(x => new DataTypeEntry
                                    {
                                        Key = x.SourceDataType,
                                        Type = x.SourceObjectType
                                    }).ToList(),
                                ExistingContainerAction = ExistingContainerActionEnum.Archive,
                                ExportIncomingEdges = stream.ExportIncomingEdges,
                                ExportOutgoingEdges = stream.ExportOutgoingEdges,
                                OldContainerName = stream.ContainerName,
                            };

                            _logger.LogInformation($"Setting {nameof(StreamMode.EventStream)} for stream '{{StreamName}}' ({{StreamId}})", stream.Name, stream.Id);

                            await streamRepository.SetupConnector(executionContext, stream.Id, model);
                        }
                    }
                }
            }

            dbContext.Settings.Add(new Setting
            {
                Id = Guid.NewGuid(),
                OrganizationId = Guid.Empty,
                UserId = Guid.Empty,
                Key = upgradeSettingKey,
                Data = "Complete",
            });
            await dbContext.SaveChangesAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"{_componentName}: Upgrade error");
        }
    }
}
