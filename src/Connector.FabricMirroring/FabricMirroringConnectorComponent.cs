using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FabricMirroring.Connector;
using CluedIn.Core;

using ComponentHost;

namespace CluedIn.Connector.FabricMirroring;

[Component(nameof(FabricMirroringConnectorComponent), "Providers", ComponentType.Service,
    ServerComponents.ProviderWebApi,
    Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
public sealed class FabricMirroringConnectorComponent : DataLakeConnectorComponentBase
{
    public FabricMirroringConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
    {
        Container.Install(new InstallComponents());
    }

    /// <summary>Starts this instance.</summary>
    public override void Start()
    {
        DefaultStartInternal<IFabricMirroringConstants, FabricMirroringJobDataFactory, FabricMirroringExportEntitiesJob>();
    }

    public const string ComponentName = "FabricMirroring";

    protected override string ConnectorComponentName => ComponentName;

    protected override string ShortConnectorComponentName => ComponentName;
}
