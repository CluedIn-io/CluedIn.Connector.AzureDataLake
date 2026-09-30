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
    // May be a pattern (e.g. "{ContainerName}_Table") - callers resolve it via
    // PatternHelper.ReplaceNameUsingPatternAsync (see SnowflakeExportEntitiesJob/
    // SnowflakeConnector.ArchiveContainer) before using it as an actual Snowflake object
    // name. {DataTime} is deliberately not supported for this pattern - unlike a file name,
    // the resolved name is expected to stay the same across export runs (see
    // GetTransientTableName/GetPipeName below), which only holds for the other variables
    // (StreamId, ContainerName, OutputFormat).
    public string TableName => GetConfigurationTrimmedStringValue(SnowflakeConfigurationConstants.TableName);

    // Snowpipe Streaming pipes are bound to a fixed target table, so the transient/landing
    // table and its pipe use stable, deterministic names per (resolved) target table rather
    // than a fresh name per export run. A leftover transient table from a crashed run is
    // recognised by this same name and its rows are cleared out at the start of the next
    // run. Takes the already-resolved table name (not the raw pattern) so these track
    // whatever the actual MERGE target is.
    public static string GetTransientTableName(string resolvedTableName) => $"{resolvedTableName}__CLUEDIN_TRANSIENT";
    public static string GetPipeName(string resolvedTableName) => $"{resolvedTableName}__CLUEDIN_PIPE";

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
