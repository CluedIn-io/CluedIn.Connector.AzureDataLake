using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.FabricOpenMirroring;

internal class OpenMirroringConnectorJobData : DataLakeJobData
{
    public OpenMirroringConnectorJobData(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string WorkspaceName => GetConfigurationValue(OpenMirroringConstants.WorkspaceName) as string;
    public string MirroredDatabaseName => GetConfigurationValue(OpenMirroringConstants.MirroredDatabaseName) as string ?? string.Empty;
    public string ClientId => GetConfigurationValue(OpenMirroringConstants.ClientId) as string;
    public string ClientSecret => GetConfigurationValue(OpenMirroringConstants.ClientSecret) as string;
    public string TenantId => GetConfigurationValue(OpenMirroringConstants.TenantId) as string;
    public override bool ShouldWriteGuidAsString => true;
    public override bool ShouldEscapeVocabularyKeys => true;
    public override bool IsDeltaMode => true;
    public override bool IsOverwriteEnabled => false;
    public override bool IsSerializedArrayColumnsEnabled => false;
    public virtual bool ShouldCreateMirroredDatabase => GetConfigurationValue(OpenMirroringConstants.ShouldCreateMirroredDatabase) as bool? ?? false;

    protected override void AddToHashCode(HashCode hash)
    {
        hash.Add(WorkspaceName);
        hash.Add(MirroredDatabaseName);
        hash.Add(ClientId);
        hash.Add(ClientSecret);
        hash.Add(TenantId);
        hash.Add(ShouldCreateMirroredDatabase);

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as OpenMirroringConnectorJobData);
    }

    public bool Equals(OpenMirroringConnectorJobData other)
    {
        return other != null &&
            WorkspaceName == other.WorkspaceName &&
            MirroredDatabaseName == other.MirroredDatabaseName &&
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
