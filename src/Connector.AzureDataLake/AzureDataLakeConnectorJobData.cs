using System;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureDataLake
{
    public class AzureDataLakeConnectorJobData : CrawlJobDataWrapper
    {
        public AzureDataLakeConnectorJobData(IDictionary<string, object> configurations, string containerName = null, IDictionary<string, object> connectorProperties = null) : base(configurations)
        {
            ContainerName = containerName;
            ConnectorProperties = connectorProperties;
        }

        public string AccountName => Configurations[AzureDataLakeConstants.AccountName] as string;
        public string AccountKey => Configurations[AzureDataLakeConstants.AccountKey] as string;
        public string FileSystemName => Configurations[AzureDataLakeConstants.FileSystemName] as string;
        public string DirectoryName => Configurations[AzureDataLakeConstants.DirectoryName] as string;
        public string ContainerName { get; }
        public IDictionary<string, object> ConnectorProperties { get; }

        public override int GetHashCode()
        {
            return HashCode.Combine(AccountName, AccountKey, FileSystemName, DirectoryName, ContainerName);
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
                DirectoryName == other.DirectoryName &&
                ContainerName == other.ContainerName;
                
        }
    }
}
