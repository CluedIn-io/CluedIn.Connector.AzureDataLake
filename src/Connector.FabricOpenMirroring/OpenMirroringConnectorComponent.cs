using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FabricOpenMirroring.Connector;
using CluedIn.Connector.FabricOpenMirroring.EventHandlers;
using CluedIn.Core;

using ComponentHost;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FabricOpenMirroring;

[Component(nameof(OpenMirroringConnectorComponent), "Providers", ComponentType.Service,
    ServerComponents.ProviderWebApi,
    Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
public sealed class OpenMirroringConnectorComponent : DataLakeConnectorComponentBase
{
    private ExportTargetEventHandler _exportTargetEventHandler;
    public OpenMirroringConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
    {
        Container.Install(new InstallComponents());
    }

    /// <summary>Starts this instance.</summary>
    public override void Start()
    {
        DefaultStartInternal<IOpenMirroringConstants, OpenMirroringJobDataFactory, OpenMirroringExportEntitiesJob>();
    }

    public const string ComponentName = "FabricOpenMirroring";

    protected override string ConnectorComponentName => ComponentName;

    protected override string ShortConnectorComponentName => ComponentName;

    private protected override void SubscribeToEvents(IDataLakeConstants constants, IDataLakeJobDataFactory jobDataFactory, IScheduledJobQueue jobQueue)
    {
        var logger = Container.Resolve<ILogger<ExportTargetEventHandler>>();
        var dateTimeProvider = Container.Resolve<IDateTimeOffsetProvider>();
        var fabricClient = Container.Resolve<OpenMirroringClient>();
        _exportTargetEventHandler = new(logger, ApplicationContext, constants, jobDataFactory, fabricClient);
        base.SubscribeToEvents(constants, jobDataFactory, jobQueue);
    }
}
