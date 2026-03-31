using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.AzureDatabricks;

internal class AzureDatabricksConnectorConfiguration : StorageConfigurationBase, IAzureServicePrincipalCredentialConfiguration
{
    public AzureDatabricksConnectorConfiguration(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string WorkspaceName => Configurations[AzureDatabricksConfigurationConstants.WorkspaceName] as string;
    public string ItemName => Configurations[AzureDatabricksConfigurationConstants.ItemName] as string;
    public string ItemType => Configurations[AzureDatabricksConfigurationConstants.ItemType] as string;
    public string ItemFolder => Configurations[AzureDatabricksConfigurationConstants.ItemFolder] as string;
    public string ClientId => Configurations[AzureDatabricksConfigurationConstants.ClientId] as string;
    public string ClientSecret => Configurations[AzureDatabricksConfigurationConstants.ClientSecret] as string;
    public string TenantId => Configurations[AzureDatabricksConfigurationConstants.TenantId] as string;
    public override bool ShouldWriteGuidAsString => true;
    public override bool ShouldEscapeVocabularyKeys => true;

    public virtual string FileSystemName => WorkspaceName;

    public override string RootDirectoryPath => $"{ItemName}.{ItemType}/{ItemFolder}/";

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

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as AzureDatabricksConnectorConfiguration);
    }

    public bool Equals(AzureDatabricksConnectorConfiguration other)
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
