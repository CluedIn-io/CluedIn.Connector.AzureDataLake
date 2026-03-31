using CluedIn.Connector.FabricOpenMirroring.Connector;
using CluedIn.Connector.FabricOpenMirroring.EventHandlers;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using ComponentHost;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FabricOpenMirroring;

[Component(nameof(OpenMirroringConnectorComponent), "Providers", ComponentType.Service,
    ServerComponents.ProviderWebApi,
    Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
public sealed class OpenMirroringConnectorComponent : StorageConnectorComponentBase
{
    private ExportTargetEventHandler _exportTargetEventHandler;
    public OpenMirroringConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
    {
        Container.Install(new InstallComponents());
    }

    /// <summary>Starts this instance.</summary>
    public override void Start()
    {
        DefaultStartInternal<IOpenMirroringConfigurationConstants, OpenMirroringFactory, OpenMirroringExportEntitiesJob>();
    }

    public const string ComponentName = "FabricOpenMirroring";

    protected override string ConnectorComponentName => ComponentName;

    protected override string ShortConnectorComponentName => ComponentName;

    private protected override void SubscribeToEvents(IStorageConfigurationConstants constants, IStorageFactory storageFactory, IScheduledJobQueue jobQueue)
    {
        var logger = Container.Resolve<ILogger<ExportTargetEventHandler>>();
        var dateTimeProvider = Container.Resolve<IDateTimeOffsetProvider>();
        var fabricClient = Container.Resolve<OpenMirroringClient>();
        _exportTargetEventHandler = new(logger, ApplicationContext, constants, storageFactory, fabricClient);
        base.SubscribeToEvents(constants, storageFactory, jobQueue);
    }
}
