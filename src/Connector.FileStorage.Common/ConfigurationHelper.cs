using CluedIn.Core.Configuration;

namespace CluedIn.Connector.FileStorage.Common;

/// <summary>
/// Reads the settings of the connectors. The key names and the default values come from the connector's configuration constants
/// (for example <see cref="IStorageConfigurationConstants.CacheBatchSizeKeyName"/> and
/// <see cref="IStorageConfigurationConstants.CacheBatchSizeDefaultValue"/>), and every connector reads them through here.
/// </summary>
public static class ConfigurationHelper
{
    /// <summary>
    /// Gets the value of a setting (an application setting or its environment variable), or the default value when it is not set.
    /// </summary>
    /// <typeparam name="T">The type of the setting.</typeparam>
    /// <param name="keyName">The key name of the setting. Without one (constants that have no such setting, like a mock of them)
    /// there is nothing to read, so the default value is the value.</param>
    /// <param name="defaultValue">The value to use when the setting is not set.</param>
    public static T GetValue<T>(string keyName, T defaultValue)
    {
        if (string.IsNullOrWhiteSpace(keyName))
        {
            return defaultValue;
        }

        return ConfigurationManagerEx.AppSettings.GetValue(keyName, defaultValue);
    }
}
