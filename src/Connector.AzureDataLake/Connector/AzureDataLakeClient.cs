using System;

using Azure.Storage;
using Azure.Storage.Files.DataLake;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake.Connector;

internal class AzureDataLakeClient : DataLakeClient
{
    private readonly DataLakeJobData _dataLakeJobData;

    public AzureDataLakeClient(ILogger<DataLakeClient> logger, DataLakeJobData dataLakeJobData) : base(logger, dataLakeJobData)
    {
        _dataLakeJobData = dataLakeJobData;
    }

    protected override DataLakeServiceClient GetDataLakeServiceClient()
    {
        var casted = CastJobData<AzureDataLakeConnectorJobData>(_dataLakeJobData);
        return new DataLakeServiceClient(
            new Uri($"https://{casted.AccountName}.dfs.core.windows.net"),
            new StorageSharedKeyCredential(casted.AccountName, casted.AccountKey));
    }
}
