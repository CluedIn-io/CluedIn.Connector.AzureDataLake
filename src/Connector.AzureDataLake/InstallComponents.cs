using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.Common.Caching;
using CluedIn.Core.Connectors;
using System.Collections.Generic;
using System.Linq;

namespace CluedIn.Connector.AzureDataLake
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<IAzureDataLakeClient>().ImplementedBy<AzureDataLakeClient>().OnlyNewServices());
            container.Register(Component.For<IAzureDataLakeConstants>().ImplementedBy<AzureDataLakeConstants>().LifestyleSingleton());
            container.Register(Component.For<ICachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>>()
                .ImplementedBy<InMemoryCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>>()
                .LifestyleSingleton());

            var ADLConnector = container.ResolveAll<IConnector>().Single(c => c.GetType() == typeof(AzureDataLakeConnector)) as AzureDataLakeConnector;
            container.Register(Component.For<IScheduledSyncs>().Instance(ADLConnector).Named($"{nameof(IScheduledSyncs)}.{nameof(AzureDataLakeConnector)}"));
        }
    }
}
