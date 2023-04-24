using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.Common.Caching;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<IAzureDataLakeClient>().ImplementedBy<AzureDataLakeClient>().OnlyNewServices());
            container.Register(Component.For<IAzureDataLakeConstants>().ImplementedBy<AzureDataLakeConstants>().LifestyleSingleton());
            container.Register(Component.For<ICachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>>()
                .UsingFactoryMethod(x =>
                {
                    var t = typeof(SqlServerCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>);
                    var factoryMethodInfo = t.GetMethod("CreateCachingService", BindingFlags.Public | BindingFlags.Static);

                    var parameters = factoryMethodInfo.GetParameters();

                    Task<SqlServerCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>> createTask;

                    if (parameters.Length == 0) // 3.3.0
                    {
                        createTask = (Task<SqlServerCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>>)factoryMethodInfo.Invoke(null, null);
                    } else if (parameters.Length == 1 && parameters[0].ParameterType == typeof(string))    // 3.4.0
                    {
                        createTask = (Task<SqlServerCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>>)factoryMethodInfo.Invoke(null, new object[]{ nameof(AzureDataLakeConnector) });
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }

                    return createTask.GetAwaiter().GetResult();
                })
                .LifestyleSingleton());
        }
    }
}
