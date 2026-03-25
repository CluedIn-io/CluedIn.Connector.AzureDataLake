using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector;

internal class AzureDataLakeClient : DataLakeClient
{

    public AzureDataLakeClient(ILogger<DataLakeClient> logger, DataLakeJobData dataLakeJobData) : base(logger, dataLakeJobData)
    {
    }
}
