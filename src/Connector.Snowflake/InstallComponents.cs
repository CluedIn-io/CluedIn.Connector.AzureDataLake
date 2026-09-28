using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.Snowflake.Connector;

namespace CluedIn.Connector.Snowflake;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<SnowflakeExportEntitiesJob, ISnowflakeConfigurationConstants, SnowflakeConfigurationConstants, SnowflakeStorageFactory>(container, store);
    }
}
