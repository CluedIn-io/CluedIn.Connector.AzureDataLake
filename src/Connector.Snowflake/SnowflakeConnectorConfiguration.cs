using System;
using System.Collections.Generic;

using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.Snowflake;

internal class SnowflakeConnectorConfiguration : StorageConfigurationBase
{
    public SnowflakeConnectorConfiguration(
        IDictionary<string, object> configurations,
        string containerName = null)
        : base(configurations, containerName)
    {
    }

    public string Account => GetConfigurationTrimmedStringValue(SnowflakeConfigurationConstants.Account);
    public string User => GetConfigurationTrimmedStringValue(SnowflakeConfigurationConstants.User);
    public string PrivateKey => GetConfigurationValue(SnowflakeConfigurationConstants.PrivateKey) as string;
    public string PrivateKeyPassphrase => GetConfigurationValue(SnowflakeConfigurationConstants.PrivateKeyPassphrase) as string;
    public string Database => GetConfigurationTrimmedStringValue(SnowflakeConfigurationConstants.Database);
    public string Schema => GetConfigurationTrimmedStringValue(SnowflakeConfigurationConstants.Schema);
    public string Warehouse => GetConfigurationTrimmedStringValue(SnowflakeConfigurationConstants.Warehouse);
    public string Role => GetConfigurationTrimmedStringValue(SnowflakeConfigurationConstants.Role);
    public string TableName => GetConfigurationTrimmedStringValue(SnowflakeConfigurationConstants.TableName);

    // Snowpipe Streaming pipes are bound to a fixed target table, so the transient/landing
    // table and its pipe use stable, deterministic names per target table rather than a
    // fresh name per export run. A leftover transient table from a crashed run is
    // recognised by this same name and its rows are cleared out at the start of the next run.
    public string TransientTableName => $"{TableName}__CLUEDIN_TRANSIENT";
    public string PipeName => $"{TableName}__CLUEDIN_PIPE";

    public override bool IsStreamCacheEnabled => true;
    public override bool ShouldWriteGuidAsString => true;
    public override bool ShouldEscapeVocabularyKeys => true;
    public override bool IsDeltaMode => true;
    public override bool IsOverwriteEnabled => false;
    public override bool IsArrayColumnsEnabled => false;

    public override string RootDirectoryPath => $"{Database}.{Schema}";

    protected override void AddToHashCode(HashCode hash)
    {
        hash.Add(Account);
        hash.Add(User);
        hash.Add(PrivateKey);
        hash.Add(PrivateKeyPassphrase);
        hash.Add(Database);
        hash.Add(Schema);
        hash.Add(Warehouse);
        hash.Add(Role);
        hash.Add(TableName);

        base.AddToHashCode(hash);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as SnowflakeConnectorConfiguration);
    }

    public bool Equals(SnowflakeConnectorConfiguration other)
    {
        return other != null &&
            Account == other.Account &&
            User == other.User &&
            PrivateKey == other.PrivateKey &&
            PrivateKeyPassphrase == other.PrivateKeyPassphrase &&
            Database == other.Database &&
            Schema == other.Schema &&
            Warehouse == other.Warehouse &&
            Role == other.Role &&
            TableName == other.TableName &&
            base.Equals(other);
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
}
