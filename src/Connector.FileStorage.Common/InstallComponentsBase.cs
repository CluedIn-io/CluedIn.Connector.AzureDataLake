using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core.Providers.ExtendedConfiguration;


namespace CluedIn.Connector.FileStorage.Common;

internal abstract class InstallComponentsBase : IWindsorInstaller
{
    public abstract void Install(IWindsorContainer container, IConfigurationStore store);

    protected static void DefaultInstall<TExportJob, TIConstants, TConstants, TClientFactory>(IWindsorContainer container, IConfigurationStore store)
        where TExportJob : StorageExportEntitiesJobBase
        where TIConstants : class, IStorageConfigurationConstants
        where TConstants : class, IStorageConfigurationConstants, TIConstants
        where TClientFactory : class, IStorageFactory
    {
        container.Register(Component.For<TExportJob>().ImplementedBy<TExportJob>().OnlyNewServices());
        container.Register(Component.For<TIConstants>().ImplementedBy<TConstants>().LifestyleSingleton());
        container.Register(Component.For<TClientFactory>().ImplementedBy<TClientFactory>().LifestyleSingleton());

        container.Register(Component.For<IExtendedConfigurationProvider>().ImplementedBy<FileStorageExtendedConfigurationProvider>().LifestyleSingleton().OnlyNewServices());
    }
}
