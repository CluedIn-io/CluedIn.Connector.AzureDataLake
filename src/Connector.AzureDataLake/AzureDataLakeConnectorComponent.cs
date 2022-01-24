using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core;
using ComponentHost;
using Connector.Common;
using System;
using System.Timers;

namespace CluedIn.Connector.AzureDataLake
{
    [Component(nameof(AzureDataLakeConnectorComponent), "Providers", ComponentType.Service,
        ServerComponents.ProviderWebApi,
        Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class AzureDataLakeConnectorComponent : ComponentBase<InstallComponents>
    {
        public AzureDataLakeConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
        }

        public override void Start()
        {
            base.Start();

            var syncService = Container.Resolve<IScheduledSyncs>();
            var backgroundExportTimer = new Timer
            {
                Interval = TimeSpan.FromMinutes(1).TotalMilliseconds,
                AutoReset = true
            };
            backgroundExportTimer.Elapsed += (_, __) => { syncService.Sync().GetAwaiter().GetResult(); };
            backgroundExportTimer.Start();
        }

        protected override string ComponentName => "Azure Data Lake Storage Gen2";

    }
}
