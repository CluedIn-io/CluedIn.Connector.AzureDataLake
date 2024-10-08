using System;
using System.Threading.Tasks;
using Azure.Storage.Files.DataLake;
using Azure.Storage;
using Newtonsoft.Json;
using Xunit;
using Parquet;
using System.Collections.Generic;
using Parquet.Schema;
using ParquetTable = Parquet.Rows.Table;
using System.IO;

namespace CluedIn.Connector.AzureDataLake.Tests.Unit
{
    public class AzureDataLakeConnectorJobDataTests
    {
        [Fact]
        public void AzureDataLakeConnectorJobData_CouldBeDeserialized()
        {
            var configString = "{\"AccountName\":\"stdlrok1\",\"AccountKey\":\"***\",\"FileSystemName\":\"newfs\",\"DirectoryName\":\"dmytroTest\",\"ContainerName\":\"TargetContainer\",\"Configurations\":{\"AccountName\":\"stdlrok1\",\"AccountKey\":\"***\",\"FileSystemName\":\"newfs\",\"DirectoryName\":\"dmytroTest\",\"firstTime\":true},\"CrawlType\":0,\"TargetHost\":null,\"TargetCredentials\":null,\"TargetApiKey\":null,\"LastCrawlFinishTime\":\"0001-01-01T00:00:00+00:00\",\"LastestCursors\":null,\"IsFirstCrawl\":false,\"ExpectedTaskCount\":0,\"IgnoreNextCrawl\":false,\"ExpectedStatistics\":null,\"ExpectedTime\":\"00:00:00\",\"ExpectedData\":0,\"Errors\":null}";
            var jobData = JsonConvert.DeserializeObject<AzureDataLake.AzureDataLakeConnectorJobData>(configString);

            Assert.NotNull(jobData.Configurations);
        }

        [Fact]
        public async Task ParquetComparison()
        {
            var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
            Assert.NotNull(accountName);
            var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
            Assert.NotNull(accountKey);

            var fileSystemName = "apacfilesystem";

            var client = new DataLakeServiceClient(new Uri($"https://{accountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(accountName, accountKey));
            var fsClient = client.GetFileSystemClient(fileSystemName);

            var data = new Dictionary<string, object>
            {
                { "id", "61087998-a109-52f5-9737-0043310f7ac6" },
                { "entitytype", "" },
                { "name", "Brighton Beach Pharmacy" },
                { "containername", "CustomerExportA" },
                { "originentitycode", "/Customer#customer_infocsv:6970" },
                { "persisthash", "bwkzhgchmec7nfz+d6yobg==" },
                { "persistversion", 1 },
                { "providerdefinitionid", "8cdd9e80-7b52-4e76-b169-b1b1728ac9c4" },
                { "customerInfoCustomerid", "6970" },
                { "customerInfoDemandgroup", "Pharmacy/HFS Other" },
                { "customerInfoName", "Brighton Beach Pharmacy" },
                { "customerInfoSiteid", "6970" },
                { "customerInfoSitename", "Brighton Beach Pharmacy" },
                { "locationAddressCity", "MERRIWA" },
            };
            var dataValueTypes = new Dictionary<string, Type>
            {
                { "id", typeof(string) },
                { "entitytype", typeof(string) },
                { "name", typeof(string) },
                { "containername", typeof(string) },
                { "originentitycode", typeof(string) },
                { "persisthash", typeof(string) },
                { "persistversion", typeof(int) },
                { "providerdefinitionid", typeof(string) },
                { "customer.Info.Customerid", typeof(string) },
                { "customer.Info.Demandgroup", typeof(string) },
                { "customer.Info.Name", typeof(string) },
                { "customer.Info.Siteid", typeof(string) },
                { "customer.Info.Sitename", typeof(string) },
                { "location.Address.City", typeof(string) },
            };

            var fields = new List<Parquet.Schema.Field>();
            var dataRow = new List<object>();
            var index = 65;
            foreach (var dataValueType in dataValueTypes)
            {
                var parquetFieldName = dataValueType.Key;
                fields.Add(new DataField(parquetFieldName, dataValueType.Value));
                dataRow.Add(data[dataValueType.Key]);
            }

            var parquetSchema = new ParquetSchema(fields);
            var parquetTable = new ParquetTable(parquetSchema)
                {
                    new Parquet.Rows.Row(dataRow)
                };

            var fileClient = fsClient.GetFileClient($"TestExport01/ChadTestExportAll.parquet");
            index++;

            var outputStream = new MemoryStream();
            using (var writer = await ParquetWriter.CreateAsync(parquetTable.Schema, outputStream))
            {
                await writer.WriteAsync(parquetTable);
            }

            outputStream.Position = 0;

            var response = await fileClient.UploadAsync(outputStream, overwrite: true);
            Assert.NotNull(response);
        }


        [Fact]
        public async Task ConvertToDeltaParquet()
        {
            var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
            Assert.NotNull(accountName);
            var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
            Assert.NotNull(accountKey);

            var fileSystemName = "apacfilesystem";

            var client = new DataLakeServiceClient(new Uri($"https://{accountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(accountName, accountKey));
            var fsClient = client.GetFileSystemClient(fileSystemName);

            await Task.FromResult(0);

            // Set up the storage account
            //CloudStorageAccount storageAccount = new CloudStorageAccount(
            //    new StorageCredentials(accountName, accountKey), true);
            //CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            //CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            //fsClient.GetPathsAsync("StreamExport");

            //using (var memoryStream = new MemoryStream())
            //{
            //    blockBlob.DownloadToStream(memoryStream);
            //    memoryStream.Position = 0;


            //    // Read Parquet file into DataFrame
            //    var df = spark.Read().Parquet();

            //    // Perform any necessary transformations
            //    // For example, df = df.Filter("some_column > some_value");

            //    var deltaTablePath = "StreamExport/TestDeltaParquet";
            //    // Write DataFrame to Delta format
            //    df.Write().Format("delta").Save(deltaTablePath);
            //}

            Console.WriteLine("Parquet file has been converted to Delta format.");
        }
    }
}
