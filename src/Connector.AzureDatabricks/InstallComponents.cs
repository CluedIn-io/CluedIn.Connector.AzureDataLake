using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.AzureDatabricks.Connector;
using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.AzureDatabricks;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<AzureDatabricksExportEntitiesJob, AzureDatabricksClient, IAzureDatabricksConstants, AzureDatabricksConstants, AzureDatabricksJobDataFactory>(container, store);
    }
}
