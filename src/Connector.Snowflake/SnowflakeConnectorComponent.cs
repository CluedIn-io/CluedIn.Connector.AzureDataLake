using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.Snowflake.Connector;
using CluedIn.Core;

using ComponentHost;

namespace CluedIn.Connector.Snowflake;

[Component(nameof(SnowflakeConnectorComponent), "Providers", ComponentType.Service,
    ServerComponents.ProviderWebApi,
    Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
public sealed class SnowflakeConnectorComponent : StorageConnectorComponentBase
{
    public SnowflakeConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
    {
        Container.Install(new InstallComponents());
    }

    public override void Start()
    {
        DefaultStartInternal<ISnowflakeConfigurationConstants, SnowflakeStorageFactory, SnowflakeExportEntitiesJob>();
    }

    public const string ComponentName = "Snowflake";

    protected override string ConnectorComponentName => ComponentName;

    protected override string ShortConnectorComponentName => ComponentName;
}
