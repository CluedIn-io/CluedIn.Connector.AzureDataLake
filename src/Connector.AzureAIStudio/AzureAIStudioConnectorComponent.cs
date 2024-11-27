using CluedIn.Connector.AzureAIStudio.Connector;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;

using ComponentHost;

namespace CluedIn.Connector.AzureAIStudio;

[Component(nameof(AzureAIStudioConnectorComponent), "Providers", ComponentType.Service,
    ServerComponents.ProviderWebApi,
    Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
public sealed class AzureAIStudioConnectorComponent : DataLakeConnectorComponentBase
{
    public AzureAIStudioConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
    {
        Container.Install(new InstallComponents());
    }

    /// <summary>Starts this instance.</summary>
    public override void Start()
    {
        DefaultStartInternal<IAzureAIStudioConstants, AzureAIStudioJobDataFactory, AzureAIStudioExportEntitiesJob>();
    }

    public const string ComponentName = "Azure AI Studio";

    protected override string ConnectorComponentName => ComponentName;
}
