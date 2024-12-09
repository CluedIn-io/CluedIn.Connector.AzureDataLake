using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.OneLake.Connector;

namespace CluedIn.Connector.OneLake;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<OneLakeExportEntitiesJob, OneLakeClient, IOneLakeConstants, OneLakeConstants, OneLakeJobDataFactory>(container, store);
    }
}
