using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake.Connector;

public class OneLakeConnector : DataLakeConnector
{
    public OneLakeConnector(
        ILogger<OneLakeConnector> logger,
        OneLakeClient client,
        IOneLakeConstants constants,
        OneLakeJobDataFactory dataLakeJobDataFactory)
        : base(logger, client, constants, dataLakeJobDataFactory)
    {
    }
}
