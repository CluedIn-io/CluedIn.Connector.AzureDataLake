using CluedIn.Connector.AmazonS3.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using ComponentHost;

namespace CluedIn.Connector.AmazonS3;

[Component(nameof(AmazonS3ConnectorComponent), "Providers", ComponentType.Service,
    ServerComponents.ProviderWebApi,
    Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
public sealed class AmazonS3ConnectorComponent : StorageConnectorComponentBase
{
    public AmazonS3ConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
    {
        Container.Install(new InstallComponents());
    }

    public override void Start()
    {
        DefaultStartInternal<IAmazonS3ConfigurationConstants, AmazonS3Factory, AmazonS3ExportEntitiesJob>();
    }

    public const string ComponentName = "Amazon S3";

    protected override string ConnectorComponentName => ComponentName;

    protected override string ShortConnectorComponentName => "S3";
}
