using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

using CluedIn.Connector.AzureAIStudio.Connector;
using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.AzureAIStudio;

internal class InstallComponents : InstallComponentsBase
{
    public override void Install(IWindsorContainer container, IConfigurationStore store)
    {
        DefaultInstall<AzureAIStudioExportEntitiesJob, AzureAIStudioClient, IAzureAIStudioConstants, AzureAIStudioConstants, AzureAIStudioJobDataFactory>(container, store);
    }
}
