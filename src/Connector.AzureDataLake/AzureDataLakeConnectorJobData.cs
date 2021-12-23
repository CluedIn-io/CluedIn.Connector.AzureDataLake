using CluedIn.Connector.Common.Configurations;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureDataLake
{
    public class AzureDataLakeConnectorJobData : CrawlJobDataWrapper
    {
        public AzureDataLakeConnectorJobData(IDictionary<string, object> configuration) : base(configuration)
        {
        }

        public string AccountName => Configurations[AzureDataLakeConstants.AccountName] as string;
        public string AccountKey => Configurations[AzureDataLakeConstants.AccountKey] as string;
        public string FileSystemName => Configurations[AzureDataLakeConstants.FileSystemName] as string;
        public string DirectoryName => Configurations[AzureDataLakeConstants.DirectoryName] as string;
    }
}
