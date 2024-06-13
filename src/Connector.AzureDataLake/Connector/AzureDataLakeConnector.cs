using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector;

public class AzureDataLakeConnector : DataLakeConnector
{
    public AzureDataLakeConnector(
        ILogger<AzureDataLakeConnector> logger,
        AzureDataLakeClient client,
        IAzureDataLakeConstants constants,
        AzureDataLakeJobDataFactory dataLakeJobDataFactory)
        : base(logger, client, constants, dataLakeJobDataFactory)
    {
    }
}
