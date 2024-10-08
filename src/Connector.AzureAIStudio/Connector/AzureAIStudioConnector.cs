using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureAIStudio.Connector;

public class AzureAIStudioConnector : DataLakeConnector
{
    public AzureAIStudioConnector(
        ILogger<AzureAIStudioConnector> logger,
        AzureAIStudioClient client,
        IAzureAIStudioConstants constants,
        AzureAIStudioJobDataFactory dataLakeJobDataFactory,
        ISystemClock systemClock)
        : base(logger, client, constants, dataLakeJobDataFactory, systemClock)
    {
    }
}
