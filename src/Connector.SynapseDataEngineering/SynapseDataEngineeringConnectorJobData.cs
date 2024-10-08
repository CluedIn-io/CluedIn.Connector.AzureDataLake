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

    public string WorkspaceName => Configurations[SynapseDataEngineeringConstants.WorkspaceName] as string;
    public string ItemName => Configurations[SynapseDataEngineeringConstants.ItemName] as string;
    public string ItemType => Configurations[SynapseDataEngineeringConstants.ItemType] as string;
    public string ItemFolder => Configurations[SynapseDataEngineeringConstants.ItemFolder] as string;
    public string ClientId => Configurations[SynapseDataEngineeringConstants.ClientId] as string;
    public string ClientSecret => Configurations[SynapseDataEngineeringConstants.ClientSecret] as string;
    public string TenantId => Configurations[SynapseDataEngineeringConstants.TenantId] as string;
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
        return Equals(obj as SynapseDataEngineeringConnectorJobData);
    }

    public bool Equals(SynapseDataEngineeringConnectorJobData other)
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
