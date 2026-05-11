using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.SynapseDataEngineering.Connector;
using CluedIn.Core;

using ComponentHost;

namespace CluedIn.Connector.SynapseDataEngineering
{
    [Component(nameof(SynapseDataEngineeringConnectorComponent), "Providers", ComponentType.Service,
        ServerComponents.ProviderWebApi,
        Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class SynapseDataEngineeringConnectorComponent : StorageConnectorComponentBase
    {

        public SynapseDataEngineeringConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
            Container.Install(new InstallComponents());
        }

        /// <summary>Starts this instance.</summary>
        public override void Start()
        {
            DefaultStartInternal<ISynapseDataEngineeringConfigurationConstants, SynapseDataEngineeringStorageFactory, SynapseDataEngineeringExportEntitiesJob>();
        }

        public const string ComponentName = "Synapse Data Engineering";

        protected override string ConnectorComponentName => ComponentName;

        protected override string ShortConnectorComponentName => "SyDataEng";
    }
}
