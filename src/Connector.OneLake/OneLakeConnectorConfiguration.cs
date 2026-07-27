using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.OneLake;

internal class OneLakeConnectorConfiguration : StorageConfigurationBase, IAzureServicePrincipalCredentialConfiguration
{
    public OneLakeConnectorConfiguration(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string WorkspaceName => GetConfigurationTrimmedStringValue(OneLakeConfigurationConstants.WorkspaceName);
    public string ItemName => GetConfigurationTrimmedStringValue(OneLakeConfigurationConstants.ItemName);
    public string ItemType => Configurations[OneLakeConfigurationConstants.ItemType] as string;
    public string ItemFolder => GetConfigurationTrimmedStringValue(OneLakeConfigurationConstants.ItemFolder) as string;
    public string ClientId => Configurations[OneLakeConfigurationConstants.ClientId] as string;
    public string ClientSecret => Configurations[OneLakeConfigurationConstants.ClientSecret] as string;
    public string TenantId => Configurations[OneLakeConfigurationConstants.TenantId] as string;
    public override bool ShouldWriteGuidAsString => true;
    public override bool ShouldEscapeVocabularyKeys => GetConfigurationValue(StorageConfigurationConstants.ShouldEscapeVocabularyKeys) as bool? ?? true;
    public virtual bool ShouldLoadToTable => GetConfigurationValue(OneLakeConfigurationConstants.ShouldLoadToTable) as bool? ?? false;
    public string TableName => GetConfigurationValue(OneLakeConfigurationConstants.TableName) as string;

    public virtual string FileSystemName => WorkspaceName;

    public override string RootDirectoryPath => $"{ItemName}.{ItemType}/{ItemFolder}";
    public virtual bool UseWorkspaceLevelPrivateLink => GetConfigurationValue(OneLakeConfigurationConstants.UseWorkspaceLevelPrivateLink) as bool? ?? false;

    public string AccountName => "onelake";

    public string StorageUri => $"https://{AccountName}.dfs.fabric.microsoft.com";

    protected override void AddToHashCode(HashCode hash)
    {
        hash.Add(WorkspaceName);
        hash.Add(ItemName);
        hash.Add(ItemType);
        hash.Add(ItemFolder);
        hash.Add(ClientId);
        hash.Add(ClientSecret);
        hash.Add(TenantId);
        hash.Add(ShouldLoadToTable);
        hash.Add(TableName);
        hash.Add(UseWorkspaceLevelPrivateLink);

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as OneLakeConnectorConfiguration);
    }

    public bool Equals(OneLakeConnectorConfiguration other)
    {
        return other != null &&
            WorkspaceName == other.WorkspaceName &&
            ItemName == other.ItemName &&
            ItemType == other.ItemType &&
            ItemFolder == other.ItemFolder &&
            ClientId == other.ClientId &&
            ClientSecret == other.ClientSecret &&
            TenantId == other.TenantId &&
            ShouldLoadToTable == other.ShouldLoadToTable &&
            TableName == other.TableName &&
            UseWorkspaceLevelPrivateLink == other.UseWorkspaceLevelPrivateLink &&
            base.Equals(other);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
}
