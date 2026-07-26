#if CLUEDIN_V47_OR_GREATER
using CluedIn.Core;
using CluedIn.Integration.PrivateServices.Configuration;

namespace CluedIn.Connector.AzureDataLake;

internal partial class AzureDataLakeExtendedConfigurationProvider
{
    protected static partial bool GetIsSaaSDeployment(ExecutionContext context)
    {
        return context.ApplicationContext.System.Configuration.GetIsSaaSDeployment();
    }
}

#endif
