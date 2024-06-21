using CluedIn.Connector.OneLake.Connector;
using CluedIn.Connector.DataLake.EventHandlers;
using CluedIn.Core;
using CluedIn.Core.Server;

using ComponentHost;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake
{
    [Component(nameof(OneLakeConnectorComponent), "Providers", ComponentType.Service,
        ServerComponents.ProviderWebApi,
        Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class OneLakeConnectorComponent : ServiceApplicationComponent<IServer>
    {
        private UpdateExportTargetEventHandler _updateExportTargetHandler;
        private ChangeStreamStateEventHandler _changeStreamStateEvent;
        private UpdateStreamEventHandler _updateStreamEvent;
        public OneLakeConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
            Container.Install(new InstallComponents());
        }

        /// <summary>Starts this instance.</summary>
        public override void Start()
        {
            var constants = new OneLakeConstants(ApplicationContext);

            var jobDataFactory = new OneLakeJobDataFactory();
            var exportEntitiesJobType = typeof(OneLakeExportEntitiesJob);
            _updateExportTargetHandler = new (ApplicationContext, constants, jobDataFactory, exportEntitiesJobType);
            _changeStreamStateEvent = new (ApplicationContext, constants, jobDataFactory, exportEntitiesJobType);
            _updateStreamEvent = new(ApplicationContext, constants, jobDataFactory, exportEntitiesJobType);

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
}
