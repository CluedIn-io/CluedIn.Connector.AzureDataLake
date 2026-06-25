using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.AmazonS3.Connector;
using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.AmazonS3;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<AmazonS3ExportEntitiesJob, IAmazonS3ConfigurationConstants, AmazonS3ConfigurationConstants, AmazonS3StorageFactory>(container, store);
    }
}
