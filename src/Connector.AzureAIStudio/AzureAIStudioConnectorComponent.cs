using CluedIn.Connector.AzureAIStudio.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using ComponentHost;

namespace CluedIn.Connector.AzureAIStudio;

[Component(nameof(AzureAIStudioConnectorComponent), "Providers", ComponentType.Service,
    ServerComponents.ProviderWebApi,
    Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
public sealed class AzureAIStudioConnectorComponent : StorageConnectorComponentBase
{
    public AzureAIStudioConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
    {
        Container.Install(new InstallComponents());
    }

    /// <summary>Starts this instance.</summary>
    public override void Start()
    {
        DefaultStartInternal<IAzureAIStudioConfigurationConstants, AzureAIStudioStorageFactory, AzureAIStudioExportEntitiesJob>();
    }

    public const string ComponentName = "Azure AI Studio";

    protected override string ConnectorComponentName => ComponentName;

    protected override string ShortConnectorComponentName => "AIStudio";
}
