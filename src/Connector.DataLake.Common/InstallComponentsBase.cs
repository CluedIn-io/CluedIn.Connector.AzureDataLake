using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.DataLake.Common.Connector;


namespace CluedIn.Connector.DataLake.Common;

internal abstract class InstallComponentsBase : IWindsorInstaller
{
    public abstract void Install(IWindsorContainer container, IConfigurationStore store);

    protected static void DefaultInstall<TExportJob, TClient, TIConstants, TConstants, TJobDataFactory>(IWindsorContainer container, IConfigurationStore store)
        where TExportJob : DataLakeExportEntitiesJobBase
        where TClient : class, IDataLakeClient
        where TIConstants : class, IDataLakeConstants
        where TConstants : class, IDataLakeConstants, TIConstants
        where TJobDataFactory : class, IDataLakeJobDataFactory
    {
        container.Register(Component.For<TExportJob>().ImplementedBy<TExportJob>().OnlyNewServices());
        container.Register(Component.For<TClient>().ImplementedBy<TClient>().OnlyNewServices());
        container.Register(Component.For<TIConstants>().ImplementedBy<TConstants>().LifestyleSingleton());
        container.Register(Component.For<TJobDataFactory>().ImplementedBy<TJobDataFactory>().LifestyleSingleton());
    }
}
