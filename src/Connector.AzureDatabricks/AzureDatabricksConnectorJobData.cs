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

    public string WorkspaceName => Configurations[AzureDatabricksConstants.WorkspaceName] as string;
    public string ItemName => Configurations[AzureDatabricksConstants.ItemName] as string;
    public string ItemType => Configurations[AzureDatabricksConstants.ItemType] as string;
    public string ItemFolder => Configurations[AzureDatabricksConstants.ItemFolder] as string;
    public string ClientId => Configurations[AzureDatabricksConstants.ClientId] as string;
    public string ClientSecret => Configurations[AzureDatabricksConstants.ClientSecret] as string;
    public string TenantId => Configurations[AzureDatabricksConstants.TenantId] as string;
    public override bool ShouldWriteGuidAsString => true;
    public override bool ShouldEscapeVocabularyKeys => true;

    protected override void AddToHashCode(HashCode hash)
    {
        hash.Add(WorkspaceName);
        hash.Add(ItemName);
        hash.Add(ItemType);
        hash.Add(ItemFolder);
        hash.Add(ClientId);
        hash.Add(ClientSecret);
        hash.Add(TenantId);

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as AzureDatabricksConnectorJobData);
    }

    public bool Equals(AzureDatabricksConnectorJobData other)
    {
        return other != null &&
            WorkspaceName == other.WorkspaceName &&
            ItemName == other.ItemName &&
            ItemType == other.ItemType &&
            ItemFolder == other.ItemFolder &&
            ClientId == other.ClientId &&
            ClientSecret == other.ClientSecret &&
            TenantId == other.TenantId &&
            base.Equals(other);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
}
