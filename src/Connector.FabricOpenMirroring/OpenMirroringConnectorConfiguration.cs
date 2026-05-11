using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.FabricOpenMirroring;

internal class OpenMirroringConnectorConfiguration : StorageConfigurationBase, IAzureServicePrincipalCredentialConfiguration
{
    public OpenMirroringConnectorConfiguration(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string WorkspaceName => GetConfigurationValue(OpenMirroringConfigurationConstants.WorkspaceName) as string;
    public string MirroredDatabaseName => GetConfigurationValue(OpenMirroringConfigurationConstants.MirroredDatabaseName) as string ?? string.Empty;
    public string ClientId => GetConfigurationValue(OpenMirroringConfigurationConstants.ClientId) as string;
    public string ClientSecret => GetConfigurationValue(OpenMirroringConfigurationConstants.ClientSecret) as string;
    public string TenantId => GetConfigurationValue(OpenMirroringConfigurationConstants.TenantId) as string;
    public override bool IsStreamCacheEnabled => true;
    public override bool ShouldWriteGuidAsString => true;
    public override bool ShouldEscapeVocabularyKeys => true;
    public override bool IsDeltaMode => true;
    public override bool IsOverwriteEnabled => false;
    public override bool IsArrayColumnsEnabled => false;
    public virtual bool ShouldCreateMirroredDatabase => GetConfigurationValue(OpenMirroringConfigurationConstants.ShouldCreateMirroredDatabase) as bool? ?? false;
    public string TableName => GetConfigurationValue(OpenMirroringConfigurationConstants.TableName) as string;

    public virtual string FileSystemName => WorkspaceName;

    public override string RootDirectoryPath => $"{MirroredDatabaseName}.MountedRelationalDatabase/Files/LandingZone";
    public virtual bool UseWorkspaceLevelPrivateLink => GetConfigurationValue(OpenMirroringConstants.UseWorkspaceLevelPrivateLink) as bool? ?? false;

    public string AccountName => "onelake";

    public string StorageUri => $"https://{AccountName}.dfs.fabric.microsoft.com";

    protected override void AddToHashCode(HashCode hash)
    {
        hash.Add(WorkspaceName);
        hash.Add(MirroredDatabaseName);
        hash.Add(ClientId);
        hash.Add(ClientSecret);
        hash.Add(TenantId);
        hash.Add(ShouldCreateMirroredDatabase);
        hash.Add(TableName);
        hash.Add(UseWorkspaceLevelPrivateLink);

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as OpenMirroringConnectorConfiguration);
    }

    public bool Equals(OpenMirroringConnectorConfiguration other)
    {
        return other != null &&
            WorkspaceName == other.WorkspaceName &&
            MirroredDatabaseName == other.MirroredDatabaseName &&
            ClientId == other.ClientId &&
            ClientSecret == other.ClientSecret &&
            TenantId == other.TenantId &&
            ShouldCreateMirroredDatabase == other.ShouldCreateMirroredDatabase &&
            TableName == other.TableName &&
            UseWorkspaceLevelPrivateLink == other.UseWorkspaceLevelPrivateLink &&
            base.Equals(other);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
}
