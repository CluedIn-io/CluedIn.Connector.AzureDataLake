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
    public string ClientId => Configurations[FabricMirroringConstants.ClientId] as string;
    public string ClientSecret => Configurations[FabricMirroringConstants.ClientSecret] as string;
    public string TenantId => Configurations[FabricMirroringConstants.TenantId] as string;
    public override bool ShouldWriteGuidAsString => true;
    public override bool ShouldEscapeVocabularyKeys => true;
    public override bool IsDeltaMode => true;
    public override bool IsOverwriteEnabled => false;
    public virtual bool ShouldCreateMirroredDatabase => false;

    protected override void AddToHashCode(HashCode hash)
    {
        hash.Add(WorkspaceName);
        hash.Add(ItemName);
        hash.Add(ClientId);
        hash.Add(ClientSecret);
        hash.Add(TenantId);
        hash.Add(ShouldCreateMirroredDatabase);

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
            ClientId == other.ClientId &&
            ClientSecret == other.ClientSecret &&
            TenantId == other.TenantId &&
            ShouldCreateMirroredDatabase == other.ShouldCreateMirroredDatabase &&
            base.Equals(other);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
}
