using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FabricMirroring.Connector;
using CluedIn.Connector.FabricMirroring.EventHandlers;
using CluedIn.Core;

using ComponentHost;

namespace CluedIn.Connector.FabricMirroring;

[Component(nameof(FabricMirroringConnectorComponent), "Providers", ComponentType.Service,
    ServerComponents.ProviderWebApi,
    Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
public sealed class FabricMirroringConnectorComponent : DataLakeConnectorComponentBase
{
    private ExportTargetEventHandler _exportTargetEventHandler;
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

    private protected override void SubscribeToEvents(IDataLakeConstants constants, IDataLakeJobDataFactory jobDataFactory, IScheduledJobQueue jobQueue)
    {
        var dateTimeProvider = Container.Resolve<IDateTimeOffsetProvider>();
        var fabricClient = Container.Resolve<FabricMirroringClient>();
        _exportTargetEventHandler = new(ApplicationContext, fabricClient, constants, jobDataFactory, dateTimeProvider);
        base.SubscribeToEvents(constants, jobDataFactory, jobQueue);
    }
}
