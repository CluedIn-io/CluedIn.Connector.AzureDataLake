using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.OneLake.Connector;

namespace CluedIn.Connector.OneLake
{
    internal class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<OneLakeExportEntitiesJob>().ImplementedBy<OneLakeExportEntitiesJob>().OnlyNewServices());
            container.Register(Component.For<OneLakeClient>().ImplementedBy<OneLakeClient>().OnlyNewServices());
            container.Register(Component.For<IOneLakeConstants>().ImplementedBy<OneLakeConstants>().LifestyleSingleton());
            container.Register(Component.For<OneLakeJobDataFactory>().ImplementedBy<OneLakeJobDataFactory>().LifestyleSingleton());
        }
    }
}
