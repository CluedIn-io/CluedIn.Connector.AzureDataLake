using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.AzureDatabricks;

internal class AzureDatabricksConnectorJobData : DataLakeJobData
{
    public AzureDatabricksConnectorJobData(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string AccountName => GetConfigurationValue(AzureDatabricksConstants.AccountName) as string;
    public string AccountKey => GetConfigurationValue(AzureDatabricksConstants.AccountKey) as string;
    public string DirectoryName => GetConfigurationValue(AzureDatabricksConstants.DirectoryName) as string;
    public string FileSystemName => GetConfigurationValue(AzureDatabricksConstants.FileSystemName) as string;

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
        return Equals(obj as AzureDatabricksConnectorJobData);
    }

    public bool Equals(AzureDatabricksConnectorJobData other)
    {
        return other != null &&
            AccountName == other.AccountName &&
            AccountKey == other.AccountKey &&
            FileSystemName == other.FileSystemName &&
            DirectoryName == other.DirectoryName &&
            base.Equals(other);

    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
}
