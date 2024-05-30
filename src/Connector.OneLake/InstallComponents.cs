using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

namespace CluedIn.Connector.OneLake
{
    internal class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
        }
    }
}
