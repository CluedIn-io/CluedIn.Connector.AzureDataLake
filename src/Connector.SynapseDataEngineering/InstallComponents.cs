using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.SynapseDataEngineering.Connector;

namespace CluedIn.Connector.SynapseDataEngineering;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<SynapseDataEngineeringExportEntitiesJob, ISynapseDataEngineeringConfigurationConstants, SynapseDataEngineeringConfigurationConstants, SynapseDataEngineeringFactory>(container, store);
    }
}
