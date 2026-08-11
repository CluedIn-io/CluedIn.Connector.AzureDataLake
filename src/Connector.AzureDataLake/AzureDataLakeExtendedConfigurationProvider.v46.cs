#if !CLUEDIN_V47_OR_GREATER
using CluedIn.Core;

namespace CluedIn.Connector.AzureDataLake;

internal partial class AzureDataLakeExtendedConfigurationProvider
{
    protected static partial bool GetIsSaaSDeployment(ExecutionContext context)
    {
        return false;
    }
}

#endif
