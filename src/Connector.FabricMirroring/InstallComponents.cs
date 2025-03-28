using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FabricMirroring.Connector;

namespace CluedIn.Connector.FabricMirroring;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<FabricMirroringExportEntitiesJob, FabricMirroringClient, IFabricMirroringConstants, FabricMirroringConstants, FabricMirroringJobDataFactory>(container, store);
    }
}
