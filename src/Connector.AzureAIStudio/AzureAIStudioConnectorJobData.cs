using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.AzureAIStudio;

internal class AzureAIStudioConnectorJobData : DataLakeJobData
{
    public AzureAIStudioConnectorJobData(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string WorkspaceName => Configurations[AzureAIStudioConstants.WorkspaceName] as string;
    public string ItemName => Configurations[AzureAIStudioConstants.ItemName] as string;
    public string ItemType => Configurations[AzureAIStudioConstants.ItemType] as string;
    public string ItemFolder => Configurations[AzureAIStudioConstants.ItemFolder] as string;
    public string ClientId => Configurations[AzureAIStudioConstants.ClientId] as string;
    public string ClientSecret => Configurations[AzureAIStudioConstants.ClientSecret] as string;
    public string TenantId => Configurations[AzureAIStudioConstants.TenantId] as string;
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
        return Equals(obj as AzureAIStudioConnectorJobData);
    }

    public bool Equals(AzureAIStudioConnectorJobData other)
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
