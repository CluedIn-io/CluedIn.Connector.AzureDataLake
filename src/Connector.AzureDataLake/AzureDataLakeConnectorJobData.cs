using CluedIn.Connector.Common.Configurations;
using System;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureDataLake
{
    public class AzureDataLakeConnectorJobData : CrawlJobDataWrapper
    {
        public AzureDataLakeConnectorJobData(IDictionary<string, object> configuration, string containerName = null) : base(configuration)
        {
            ContainerName = containerName;
        }

        public string AccountName => Configurations[AzureDataLakeConstants.AccountName] as string;
        public string AccountKey => Configurations[AzureDataLakeConstants.AccountKey] as string;
        public string FileSystemName => Configurations[AzureDataLakeConstants.FileSystemName] as string;
        public string DirectoryName => Configurations[AzureDataLakeConstants.DirectoryName] as string;
        public string ContainerName { get; }

        public override int GetHashCode()
        {
            return HashCode.Combine(AccountName, AccountKey, FileSystemName, DirectoryName);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as AzureDataLakeConnectorJobData);
        }

        public bool Equals(AzureDataLakeConnectorJobData other)
        {
            return other != null &&
                AccountName == other.AccountName &&
                AccountKey == other.AccountKey &&
                FileSystemName == other.FileSystemName &&
                DirectoryName == other.DirectoryName;
        }
    }
}
