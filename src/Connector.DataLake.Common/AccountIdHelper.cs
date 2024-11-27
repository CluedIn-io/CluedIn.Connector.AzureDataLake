using System;

namespace CluedIn.Connector.DataLake.Common;

internal class AccountIdHelper
{
    public static string Generate(Guid providerId, Guid providerDefinitionId)
    {
        return $"{providerId}_{providerDefinitionId}";
    }
}
