using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.AzureDataLake;

internal class AzureDataLakeConnectorJobData : DataLakeJobData
{
    public AzureDataLakeConnectorJobData(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string AccountName => GetConfigurationValue(AzureDataLakeConstants.AccountName) as string;
    public string AccountKey => GetConfigurationValue(AzureDataLakeConstants.AccountKey) as string;
    public string DirectoryName => GetConfigurationValue(AzureDataLakeConstants.DirectoryName) as string;

    public override string FileSystemName => GetConfigurationValue(AzureDataLakeConstants.FileSystemName) as string;
    public override string RootDirectoryPath => DirectoryName;

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
        return Equals(obj as AzureDataLakeConnectorJobData);
    }

    public bool Equals(AzureDataLakeConnectorJobData other)
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
