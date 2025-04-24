using System;
using System.Collections.Generic;

using CluedIn.Connector.DataLake.Common;

namespace CluedIn.Connector.OneLake;

internal class OneLakeConnectorJobData : DataLakeJobData
{
    public OneLakeConnectorJobData(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string WorkspaceName => Configurations[OneLakeConstants.WorkspaceName] as string;
    public string ItemName => Configurations[OneLakeConstants.ItemName] as string;
    public string ItemType => Configurations[OneLakeConstants.ItemType] as string;
    public string ItemFolder => Configurations[OneLakeConstants.ItemFolder] as string;
    public string ClientId => Configurations[OneLakeConstants.ClientId] as string;
    public string ClientSecret => Configurations[OneLakeConstants.ClientSecret] as string;
    public string TenantId => Configurations[OneLakeConstants.TenantId] as string;
    public override bool ShouldWriteGuidAsString => true;
    public override bool ShouldEscapeVocabularyKeys => true;
    public virtual bool ShouldLoadToTable => GetConfigurationValue(OneLakeConstants.ShouldLoadToTable) as bool? ?? false;
    public string TableName => GetConfigurationValue(OneLakeConstants.TableName) as string;

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

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as OneLakeConnectorJobData);
    }

    public bool Equals(OneLakeConnectorJobData other)
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
            base.Equals(other);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
}
