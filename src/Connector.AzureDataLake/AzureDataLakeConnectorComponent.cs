using CluedIn.Core;
using ComponentHost;

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

        protected override string ComponentName => "Azure Data Lake Storage Gen2";
    }
}
