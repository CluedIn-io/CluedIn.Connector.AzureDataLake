using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.FabricMirroring;

internal class FabricMirroringConnectorJobData : DataLakeJobData
{
    public FabricMirroringConnectorJobData(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string WorkspaceName => Configurations[FabricMirroringConstants.WorkspaceName] as string;
    public string ItemName => Configurations[FabricMirroringConstants.ItemName] as string;
    public string ItemType => Configurations[FabricMirroringConstants.ItemType] as string;
    public string ItemFolder => Configurations[FabricMirroringConstants.ItemFolder] as string;
    public string ClientId => Configurations[FabricMirroringConstants.ClientId] as string;
    public string ClientSecret => Configurations[FabricMirroringConstants.ClientSecret] as string;
    public string TenantId => Configurations[FabricMirroringConstants.TenantId] as string;
    public override bool ShouldWriteGuidAsString => true;
    public override bool ShouldEscapeVocabularyKeys => true;
    public override bool IsDeltaMode => true;
    public override bool IsOverwriteEnabled => false;

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
        return Equals(obj as FabricMirroringConnectorJobData);
    }

    public bool Equals(FabricMirroringConnectorJobData other)
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
