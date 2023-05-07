using CluedIn.Connector.Common.Configurations;
using System;
using System.Collections.Generic;

namespace CluedIn.Connector.AzureDataLake
{
    public class AzureDataLakeConnectorJobData : CrawlJobDataWrapper
    {
        public AzureDataLakeConnectorJobData(IDictionary<string, object> configurations, string containerName, Guid providerDefinitionId) : base(configurations)
        {
            ProviderDefinitionId = providerDefinitionId;
            ContainerName = containerName;
        }

        public string AccountName => Configurations[AzureDataLakeConstants.AccountName] as string;
        public string AccountKey => Configurations[AzureDataLakeConstants.AccountKey] as string;
        public string FileSystemName => Configurations[AzureDataLakeConstants.FileSystemName] as string;
        public string DirectoryName => Configurations[AzureDataLakeConstants.DirectoryName] as string;
        public string ContainerName { get; }
        public Guid ProviderDefinitionId { get; }

        public override int GetHashCode()
        {
            return HashCode.Combine(AccountName, AccountKey, FileSystemName, DirectoryName, ContainerName, ProviderDefinitionId);
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
                ContainerName == other.ContainerName &&
                ProviderDefinitionId == other.ProviderDefinitionId;
                
        }
    }
}
