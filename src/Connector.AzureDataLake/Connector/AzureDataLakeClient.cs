using System;

using Azure.Storage;
using Azure.Storage.Files.DataLake;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.AzureDataLake.Connector;

public class AzureDataLakeClient : DataLakeClient
{
    protected override DataLakeServiceClient GetDataLakeServiceClient(IDataLakeJobData configuration)
    {
        var casted = CastJobData<AzureDataLakeConnectorJobData>(configuration);
        return new DataLakeServiceClient(
            new Uri($"https://{casted.AccountName}.dfs.core.windows.net"),
            new StorageSharedKeyCredential(casted.AccountName, casted.AccountKey),
            new DataLakeClientOptions(version: DataLakeClientOptions.ServiceVersion.V2025_07_05));
    }
}
