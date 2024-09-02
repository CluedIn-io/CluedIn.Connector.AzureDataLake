using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.SynapseDataEngineering;

internal class SynapseDataEngineeringConnectorJobData : DataLakeJobData
{
    public SynapseDataEngineeringConnectorJobData(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string AccountName => GetConfigurationValue(SynapseDataEngineeringConstants.AccountName) as string;
    public string AccountKey => GetConfigurationValue(SynapseDataEngineeringConstants.AccountKey) as string;
    public string DirectoryName => GetConfigurationValue(SynapseDataEngineeringConstants.DirectoryName) as string;
    public string FileSystemName => GetConfigurationValue(SynapseDataEngineeringConstants.FileSystemName) as string;

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
        return Equals(obj as SynapseDataEngineeringConnectorJobData);
    }

    public bool Equals(SynapseDataEngineeringConnectorJobData other)
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
