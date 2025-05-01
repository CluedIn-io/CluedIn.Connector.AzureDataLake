using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FabricOpenMirroring.Connector;

namespace CluedIn.Connector.FabricOpenMirroring;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<OpenMirroringExportEntitiesJob, OpenMirroringClient, IOpenMirroringConstants, OpenMirroringConstants, OpenMirroringJobDataFactory>(container, store);
    }
}
