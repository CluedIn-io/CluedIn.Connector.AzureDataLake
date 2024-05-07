using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core.Providers.ExtendedConfiguration;

namespace CluedIn.Connector.AzureDataLake
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<IAzureDataLakeClient>()
                .ImplementedBy<AzureDataLakeClient>()
                .LifestyleSingleton());

            container.Register(Component.For<IAzureDataLakeConstants>()
                .ImplementedBy<AzureDataLakeConstants>()
                .LifestyleSingleton());

            container.Register(Component.For<IExtendedConfigurationProvider>()
                .ImplementedBy<AzureDataLakeExtendedConfigurationProvider>()
                .LifestyleSingleton());
        }
    }
}
