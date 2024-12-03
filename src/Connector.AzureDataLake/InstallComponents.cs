using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.AzureDataLake;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<AzureDataLakeExportEntitiesJob, AzureDataLakeClient, IAzureDataLakeConstants, AzureDataLakeConstants, AzureDataLakeJobDataFactory>(container, store);
    }
}
