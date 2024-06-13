using System;

using Azure.Identity;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake;
using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.OneLake.Connector;

public class OneLakeClient : DataLakeClient
{
    protected override DataLakeServiceClient GetDataLakeServiceClient(IDataLakeJobData configuration)
    {
        var casted = CastJobData<OneLakeConnectorJobData>(configuration);
        var accountName = "onelake";

        var sharedKeyCredential = new ClientSecretCredential(casted.TenantId, casted.ClientId, casted.ClientSecret);

        var dfsUri = $"https://{accountName}.dfs.fabric.microsoft.com";

        var dataLakeServiceClient = new DataLakeServiceClient(
            new Uri(dfsUri),
            sharedKeyCredential);
        return dataLakeServiceClient;
    }

    protected override string GetDirectory(IDataLakeJobData configuration)
    {
        var casted = CastJobData<OneLakeConnectorJobData>(configuration);
        return  $"{casted.ItemName}.{casted.ItemType}/{casted.ItemFolder}/"; //"jlalakehouse.Lakehouse/Files/";
    }

    protected override string GetFileSystemName(IDataLakeJobData configuration)
    {
        var casted = CastJobData<OneLakeConnectorJobData>(configuration);
        return casted.WorkspaceName;
    }
}
