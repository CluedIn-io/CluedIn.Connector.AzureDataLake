using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.OneLake.Connector;
using CluedIn.Core;

using ComponentHost;

using Microsoft.Extensions.Logging;

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
        var dataLakeConstants = Container.Resolve<IOneLakeConstants>();
        var jobDataFactory = Container.Resolve<OneLakeJobDataFactory>();
        var exportEntitiesJobType = typeof(OneLakeExportEntitiesJob);
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

    public const string ComponentName = "OneLake";
}
