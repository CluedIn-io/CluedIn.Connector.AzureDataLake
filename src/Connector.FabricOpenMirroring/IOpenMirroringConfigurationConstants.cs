using CluedIn.Connector.FileStorage.Common;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.FabricOpenMirroring;

public interface IOpenMirroringConfigurationConstants : IStorageConfigurationConstants
{
    /// <summary>
    /// Environment key name for the minimum interval between repeated attempts to create a mirrored database.
    /// </summary>
    string MirroredDatabaseCreationRetryIntervalKeyName { get; }

    /// <summary>
    /// Default value for mirrored database creation retry interval in minutes.
    /// </summary>
    int MirroredDatabaseCreationRetryIntervalDefaultValue { get; }
}
