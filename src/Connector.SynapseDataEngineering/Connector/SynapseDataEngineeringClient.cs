using System;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;

namespace CluedIn.Connector.SynapseDataEngineering.Connector;

public class SynapseDataEngineeringClient : DataLakeClient
{
    protected override async Task<DataLakeServiceClient> GetDataLakeServiceClientAsync(IDataLakeJobData configuration)
    {
        var casted = CastJobData<SynapseDataEngineeringConnectorJobData>(configuration);
        var accountName = "onelake";

        var sharedKeyCredential = new ClientSecretCredential(casted.TenantId, casted.ClientId, casted.ClientSecret);

        var dfsUri = $"https://{accountName}.dfs.fabric.microsoft.com";

        var dataLakeServiceClient = new DataLakeServiceClient(
            new Uri(dfsUri),
            sharedKeyCredential);
        return dataLakeServiceClient;
    }
}
