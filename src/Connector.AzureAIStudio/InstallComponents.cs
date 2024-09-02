using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.AzureAIStudio.Connector;

namespace CluedIn.Connector.AzureAIStudio
{
    internal class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<AzureAIStudioExportEntitiesJob>().ImplementedBy<AzureAIStudioExportEntitiesJob>().OnlyNewServices());
            container.Register(Component.For<AzureAIStudioClient>().ImplementedBy<AzureAIStudioClient>().OnlyNewServices());
            container.Register(Component.For<IAzureAIStudioConstants>().ImplementedBy<AzureAIStudioConstants>().LifestyleSingleton());
            container.Register(Component.For<AzureAIStudioJobDataFactory>().ImplementedBy<AzureAIStudioJobDataFactory>().LifestyleSingleton());
        }
    }
}
