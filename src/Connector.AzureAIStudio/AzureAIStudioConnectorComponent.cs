using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.AzureAIStudio.Connector;
using CluedIn.Core;

using ComponentHost;

using Microsoft.Extensions.Logging;

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
        var dataLakeConstants = Container.Resolve<IAzureAIStudioConstants>();
        var jobDataFactory = Container.Resolve<AzureAIStudioJobDataFactory>();
        var exportEntitiesJobType = typeof(AzureAIStudioExportEntitiesJob);
        SubscribeToEvents(dataLakeConstants, exportEntitiesJobType);
        _ = Task.Run(() => RunScheduler(dataLakeConstants, jobDataFactory, exportEntitiesJobType));

        Log.LogInformation($"{ComponentName} Registered");
        State = ServiceState.Started;
    }

    /// <summary>Stops this instance.</summary>
    public override void Stop()
    {
        if (State == ServiceState.Stopped)
        {
            return;
        }

        State = ServiceState.Stopped;
    }

    public const string ComponentName = "Azure AI Studio";
}
