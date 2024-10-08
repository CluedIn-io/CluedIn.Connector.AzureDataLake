using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.AzureDatabricks.Connector;

namespace CluedIn.Connector.AzureDatabricks
{
    internal class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<AzureDatabricksExportEntitiesJob>().ImplementedBy<AzureDatabricksExportEntitiesJob>().OnlyNewServices());
            container.Register(Component.For<AzureDatabricksClient>().ImplementedBy<AzureDatabricksClient>().OnlyNewServices());
            container.Register(Component.For<IAzureDatabricksConstants>().ImplementedBy<AzureDatabricksConstants>().LifestyleSingleton());
            container.Register(Component.For<AzureDatabricksJobDataFactory>().ImplementedBy<AzureDatabricksJobDataFactory>().LifestyleSingleton());
        }
    }
}
