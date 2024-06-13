using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.AzureDataLake.Connector;

namespace CluedIn.Connector.AzureDataLake
{
    internal class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<AzureDataLakeExportEntitiesJob>().ImplementedBy<AzureDataLakeExportEntitiesJob>().OnlyNewServices());
            container.Register(Component.For<AzureDataLakeClient>().ImplementedBy<AzureDataLakeClient>().OnlyNewServices());
            container.Register(Component.For<IAzureDataLakeConstants>().ImplementedBy<AzureDataLakeConstants>().LifestyleSingleton());
            container.Register(Component.For<AzureDataLakeJobDataFactory>().ImplementedBy<AzureDataLakeJobDataFactory>().LifestyleSingleton());
        }
    }
}
