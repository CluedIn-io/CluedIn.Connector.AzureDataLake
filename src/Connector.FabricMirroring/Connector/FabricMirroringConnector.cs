using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Azure.Core;
using Azure.Identity;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;

using Microsoft.Extensions.Logging;
using Microsoft.Fabric.Api;


namespace CluedIn.Connector.FabricMirroring.Connector;

public class FabricMirroringConnector : DataLakeConnector
{
    public FabricMirroringConnector(
        ILogger<FabricMirroringConnector> logger,
        FabricMirroringClient client,
        IFabricMirroringConstants constants,
        FabricMirroringJobDataFactory dataLakeJobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, client, constants, dataLakeJobDataFactory, dateTimeOffsetProvider)
    {
    }

    public override async Task<ConnectionVerificationResult> VerifyConnection(ExecutionContext executionContext, IReadOnlyDictionary<string, object> config)
    {
        //var jobData = await DataLakeJobDataFactory.GetConfiguration(executionContext, new Dictionary<string, object>(config)) as FabricMirroringConnectorJobData;
        //var sharedKeyCredential = new ClientSecretCredential(jobData.TenantId, jobData.ClientId, jobData.ClientSecret);

        ////var fabricAdminClient = new FabricAdminClient(sharedKeyCredential);
        //var fabricNonAdminClient = new FabricClient(sharedKeyCredential);

        //var fabricClient = fabricNonAdminClient;
        //// Get the list of workspaces using the client
        //var workspaces = fabricClient.Core.Workspaces.ListWorkspaces().ToList();
        //Console.WriteLine("Number of workspaces: " + workspaces.Count);
        //foreach (var workspace in workspaces)
        //{
        //    Console.WriteLine($"Workspace: {workspace.DisplayName}, Capacity ID: {workspace.CapacityId}");
        //}
        //var accountName = "onelake";

        //var dfsUri = $"https://onelake.dfs.fabric.microsoft.com/";

        //var dataLakeServiceClient = new DataLakeServiceClient(
        //    new Uri(dfsUri),
        //    sharedKeyCredential);

        //{
        //    await foreach (var item in dataLakeServiceClient.GetFileSystemsAsync())
        //    {
        //        Console.WriteLine("FIleSystems: " + item.Name);
        //        var fileSystemClient = dataLakeServiceClient.GetFileSystemClient(item.Name);
        //        await foreach (var dir in fileSystemClient.GetPathsAsync())
        //        {
        //            Console.WriteLine("dir: " + dir.Name);
        //        }

        //    }
        //}

        //{
        //    var fileSystemClient = dataLakeServiceClient.GetFileSystemClient(jobData.WorkspaceName);
        //    var directoryClient = fileSystemClient.GetDirectoryClient("CluedIn_Mirroring.MountedRelationalDatabase/Files/LandingZone/peng");

        //    await foreach (var item in directoryClient.GetPathsAsync())
        //    {
        //        Console.WriteLine("File1: " + item.Name);
        //    }

        //    var fileClient = directoryClient.GetFileClient("_manifest.json");
        //    var fileClient2 = directoryClient.GetFileClient("test.json");
        //    //fileClient2.CreateIfNotExists();
        //    var stream = fileClient2.OpenWrite(true);
        //    stream.Write(Encoding.UTF8.GetBytes("sssss.sdfd"));
        //    await stream.FlushAsync();
        //    var fileProperties = await fileClient.GetPropertiesAsync();

        //    Console.WriteLine("Fileprop1: " + fileProperties.Value.ContentLength);
        //    Console.WriteLine("Fileprop1: " + fileProperties.Value.ContentDisposition);
        //}

        //{
        //    var fileSystemClient = dataLakeServiceClient.GetFileSystemClient(jobData.WorkspaceName);
        //    var directoryClient = fileSystemClient.GetDirectoryClient("b0af7e2a-62ab-4800-8c17-2e3e50621a05/bd87a442-0e1e-471f-b1dd-ae667f000f4c/Files/LandingZone/peng");

        //    await foreach (var item in directoryClient.GetPathsAsync())
        //    {
        //        Console.WriteLine("File2: " + item.Name);
        //    }

        //    var fileClient = directoryClient.GetFileClient("_manifest.json");
        //    var fileProperties = await fileClient.GetPropertiesAsync();

        //    Console.WriteLine("Fileprop2: " + fileProperties.Value.ContentLength);
        //    Console.WriteLine("Fileprop2: " + fileProperties.Value.ContentDisposition);
        //}

        return await base.VerifyConnection(executionContext, config);
    }
}
