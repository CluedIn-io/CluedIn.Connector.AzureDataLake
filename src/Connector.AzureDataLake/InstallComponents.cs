using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core.Providers.ExtendedConfiguration;

namespace CluedIn.Connector.AzureDataLake;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<AzureDataLakeExportEntitiesJob, IAzureDataLakeConfigurationConstants, AzureDataLakeConfigurationConstants, AzureDataLakeStorageFactory>(container, store);
        container.Register(Component.For<IExtendedConfigurationProvider>().ImplementedBy<AzureDataLakeExtendedConfigurationProvider>().LifestyleSingleton());

    }
}
