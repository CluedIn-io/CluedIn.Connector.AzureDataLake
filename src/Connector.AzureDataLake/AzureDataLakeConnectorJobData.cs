using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Connectors;

using static CluedIn.Connector.AzureDataLake.AzureDataLakeConstants;

namespace CluedIn.Connector.AzureDataLake
{
    public class AzureDataLakeConnectorJobData : CrawlJobDataWrapper
    {
        private const string StreamCacheConnectionStringKey = "StreamCache";

        public AzureDataLakeConnectorJobData(IDictionary<string, object> configurations, string containerName = null) : base(configurations)
        {
            ContainerName = containerName;
        }

        public string AccountName => GetConfigurationValue(AzureDataLakeConstants.AccountName) as string;
        public string AccountKey => GetConfigurationValue(AzureDataLakeConstants.AccountKey) as string;
        public string FileSystemName => GetConfigurationValue(AzureDataLakeConstants.FileSystemName) as string;
        public string DirectoryName => GetConfigurationValue(AzureDataLakeConstants.DirectoryName) as string;
        public string OutputFormat => GetConfigurationValue(AzureDataLakeConstants.OutputFormat) as string ?? OutputFormats.Json;
        public bool IsStreamCacheEnabled => GetConfigurationValue(AzureDataLakeConstants.IsStreamCacheEnabled) as bool? ?? false;
        public string StreamCacheConnectionString => GetConfigurationValue(AzureDataLakeConstants.StreamCacheConnectionString) as string;
        public string Schedule => GetConfigurationValue(AzureDataLakeConstants.Schedule) as string;
        public string ContainerName { get; }
        public bool UseCurrentTimeForExport => GetConfigurationValue(AzureDataLakeConstants.UseCurrentTimeForExport) as bool? ?? false;

        public override int GetHashCode()
        {
            HashCode hash = new();
            hash.Add(AccountName);
            hash.Add(AccountKey);
            hash.Add(FileSystemName);
            hash.Add(DirectoryName);
            hash.Add(OutputFormat);
            hash.Add(IsStreamCacheEnabled);
            hash.Add(StreamCacheConnectionString);
            hash.Add(Schedule);
            hash.Add(ContainerName);
            hash.Add(UseCurrentTimeForExport);
            return hash.ToHashCode();
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
                OutputFormat == other.OutputFormat &&
                IsStreamCacheEnabled == other.IsStreamCacheEnabled &&
                StreamCacheConnectionString == other.StreamCacheConnectionString &&
                Schedule == other.Schedule &&
                UseCurrentTimeForExport == other.UseCurrentTimeForExport;
                
        }

        private object GetConfigurationValue(string key)
        {
            if (Configurations.TryGetValue(key, out var value))
            {
                return value;
            }
            return null;
        }

        public static async Task<AzureDataLakeConnectorJobData> Create(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName = null)
        {
            var authenticationDetails = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            return await Create(executionContext, authenticationDetails.Authentication.ToDictionary(detail => detail.Key, detail => detail.Value), containerName);
        }

        public static Task<AzureDataLakeConnectorJobData> Create(
            ExecutionContext executionContext,
            IDictionary<string, object> authenticationDetails,
            string containerName = null)
        {
            var connectionStrings = executionContext.ApplicationContext.System.ConnectionStrings;
            if (connectionStrings.ConnectionStringExists(StreamCacheConnectionStringKey))
            {
                authenticationDetails[AzureDataLakeConstants.StreamCacheConnectionString] = connectionStrings.GetConnectionString(StreamCacheConnectionStringKey);
            }
            else if (!authenticationDetails.ContainsKey(AzureDataLakeConstants.StreamCacheConnectionString))
            {
                authenticationDetails[AzureDataLakeConstants.StreamCacheConnectionString] = null;
            } 

            var configurations = new AzureDataLakeConnectorJobData(authenticationDetails, containerName);
            return Task.FromResult(configurations);
        }

        private static async Task<IConnectorConnectionV2> GetAuthenticationDetails(
            ExecutionContext executionContext,
            Guid providerDefinitionId)
        {
            return await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, providerDefinitionId);
        }
    }
}
