using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.SynapseDataEngineering.Connector;

namespace CluedIn.Connector.SynapseDataEngineering
{
    internal class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<SynapseDataEngineeringExportEntitiesJob>().ImplementedBy<SynapseDataEngineeringExportEntitiesJob>().OnlyNewServices());
            container.Register(Component.For<SynapseDataEngineeringClient>().ImplementedBy<SynapseDataEngineeringClient>().OnlyNewServices());
            container.Register(Component.For<ISynapseDataEngineeringConstants>().ImplementedBy<SynapseDataEngineeringConstants>().LifestyleSingleton());
            container.Register(Component.For<SynapseDataEngineeringJobDataFactory>().ImplementedBy<SynapseDataEngineeringJobDataFactory>().LifestyleSingleton());
        }
    }
}
