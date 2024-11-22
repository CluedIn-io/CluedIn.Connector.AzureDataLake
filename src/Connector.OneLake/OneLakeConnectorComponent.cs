using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.OneLake.Connector;
using CluedIn.Core;

using ComponentHost;

namespace CluedIn.Connector.OneLake;

[Component(nameof(OneLakeConnectorComponent), "Providers", ComponentType.Service,
    ServerComponents.ProviderWebApi,
    Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
public sealed class OneLakeConnectorComponent : DataLakeConnectorComponentBase
{
    public OneLakeConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
    {
        Container.Install(new InstallComponents());
    }

    /// <summary>Starts this instance.</summary>
    public override void Start()
    {
        DefaultStartInternal<IOneLakeConstants, OneLakeJobDataFactory, OneLakeExportEntitiesJob>();
    }

    public const string ComponentName = "OneLake";

    protected override string ConnectorComponentName => ComponentName;
}
