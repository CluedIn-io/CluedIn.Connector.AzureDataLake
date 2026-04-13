using CluedIn.Connector.AzureDatabricks.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using ComponentHost;

namespace CluedIn.Connector.AzureDatabricks
{
    [Component(nameof(AzureDatabricksConnectorComponent), "Providers", ComponentType.Service,
        ServerComponents.ProviderWebApi,
        Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class AzureDatabricksConnectorComponent : StorageConnectorComponentBase
    {

        public AzureDatabricksConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
            Container.Install(new InstallComponents());
        }

        /// <summary>Starts this instance.</summary>
        public override void Start()
        {
            DefaultStartInternal<IAzureDatabricksConfigurationConstants, AzureDatabricksStorageFactory, AzureDatabricksExportEntitiesJob>();
        }

        public const string ComponentName = "Azure Databricks";

        protected override string ConnectorComponentName => ComponentName;

        protected override string ShortConnectorComponentName => "Databricks";
    }
}
