using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.AmazonS3.Connector;
using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.AmazonS3;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<AmazonS3ExportEntitiesJob, AmazonS3StorageClient, IAmazonS3Constants, AmazonS3Constants, AmazonS3JobDataFactory>(container, store);
    }
}
