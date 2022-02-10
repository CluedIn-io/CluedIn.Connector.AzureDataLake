using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.Common.Caching;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureDataLake
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<IAzureDataLakeClient>().ImplementedBy<AzureDataLakeClient>().OnlyNewServices());
            container.Register(Component.For<IAzureDataLakeConstants>().ImplementedBy<AzureDataLakeConstants>().LifestyleSingleton());
            container.Register(Component.For<ICachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>>()
                .UsingFactoryMethod(x => SqlServerCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>.CreateCachingService(nameof(AzureDataLakeConnector)).GetAwaiter().GetResult())
                .LifestyleSingleton());

            container.Register(Component.For<IScheduledSyncs>().ImplementedBy<AzureDataLakeConnector>().Named($"{nameof(IScheduledSyncs)}.{nameof(AzureDataLakeConnector)}").LifestyleSingleton());
        }
    }
}
