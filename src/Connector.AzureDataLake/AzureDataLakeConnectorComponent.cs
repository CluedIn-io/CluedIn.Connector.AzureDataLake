using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Configuration;
using ComponentHost;
using Connector.Common;
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

            var configurations = Container.Resolve<IAzureDataLakeConstants>();
            var cacheIntervalValue = ConfigurationManagerEx.AppSettings.GetValue(configurations.CacheSyncIntervalKeyName, configurations.CacheSyncIntervalDefaultValue);
            var syncService = Container.Resolve<IScheduledSyncs>();
            var backgroundCacheSyncTimer = new Timer
            {
                Interval = cacheIntervalValue,
                AutoReset = true
            };
            backgroundCacheSyncTimer.Elapsed += (_, __) => { syncService.Sync().GetAwaiter().GetResult(); };
            backgroundCacheSyncTimer.Start();
        }

        protected override string ComponentName => "Azure Data Lake Storage Gen2";

    }
}
