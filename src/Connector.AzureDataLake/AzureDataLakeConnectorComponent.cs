using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.DataStore.Entities;

using ComponentHost;

using Microsoft.EntityFrameworkCore;

namespace CluedIn.Connector.AzureDataLake
{
    [Component(nameof(AzureDataLakeConnectorComponent), "Providers", ComponentType.Service,
        ServerComponents.ProviderWebApi,
        Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class AzureDataLakeConnectorComponent : DataLakeConnectorComponentBase
    {
        public AzureDataLakeConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
            Container.Install(new InstallComponents());
        }

        public override void Start()
        {
            DefaultStartInternal<IAzureDataLakeConstants, AzureDataLakeJobDataFactory, AzureDataLakeExportEntitiesJob>();
        }

        private protected override IDataMigrator GetDataMigrator(IDataLakeConstants constants, IDataLakeJobDataFactory jobDataFactory)
        {
            return new AzureDataLakeDataMigrator(
                Log,
                ApplicationContext,
                Container.Resolve<DbContextOptions<CluedInEntities>>(),
                ConnectorComponentName,
                constants,
                jobDataFactory);
        }

        public const string ComponentName = "Azure Data Lake Storage Gen2";

        protected override string ConnectorComponentName => ComponentName;
    }
}
