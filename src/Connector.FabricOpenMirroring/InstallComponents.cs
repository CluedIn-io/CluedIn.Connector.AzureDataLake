using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FabricOpenMirroring.Connector;

namespace CluedIn.Connector.FabricOpenMirroring;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<OpenMirroringExportEntitiesJob, IOpenMirroringConfigurationConstants, OpenMirroringConfigurationConstants, OpenMirroringStorageFactory>(container, store);
    }
}
