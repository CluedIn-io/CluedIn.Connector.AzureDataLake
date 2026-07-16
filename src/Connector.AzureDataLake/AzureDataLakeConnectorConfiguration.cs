using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.AzureDataLake;

internal class AzureDataLakeConnectorConfiguration : StorageConfigurationBase, IAzureSharedKeyCredentialConfiguration
{
    public AzureDataLakeConnectorConfiguration(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string AccountName => GetConfigurationValue(AzureDataLakeConfigurationConstants.AccountName) as string;
    public string AccountKey => GetConfigurationValue(AzureDataLakeConfigurationConstants.AccountKey) as string;
    public string DirectoryName => GetConfigurationValue(AzureDataLakeConfigurationConstants.DirectoryName) as string;

    public virtual string FileSystemName => GetConfigurationValue(AzureDataLakeConfigurationConstants.FileSystemName) as string;
    public override string RootDirectoryPath => DirectoryName;

    public string StorageUri => $"https://{AccountName}.dfs.core.windows.net";

    protected override void AddToHashCode(HashCode hash)
    {
        hash.Add(AccountName);
        hash.Add(AccountKey);
        hash.Add(FileSystemName);
        hash.Add(DirectoryName);

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as AzureDataLakeConnectorConfiguration);
    }

    public bool Equals(AzureDataLakeConnectorConfiguration other)
    {
        return other != null &&
            AccountName == other.AccountName &&
            AccountKey == other.AccountKey &&
            FileSystemName == other.FileSystemName &&
            RootDirectoryPath == other.RootDirectoryPath &&
            base.Equals(other);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
}
