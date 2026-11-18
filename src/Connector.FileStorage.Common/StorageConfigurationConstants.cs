using System;
using System.Collections.Generic;

using CluedIn.Core;
using CluedIn.Core.Providers;
using CluedIn.Core.Streams.Models;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.FileStorage.Common;

public abstract class StorageConfigurationConstants : ConfigurationConstantsBase, IStorageConfigurationConstants
{
    internal const string ProviderDefinitionIdKey = "__ProviderDefinitionId__";
    internal const string ChangeTypeKey = "__ChangeType__";
    internal const string PersistVersionKey = "PersistVersion";
    internal const string TimestampKey = "Timestamp";
    internal const string EpochKey = "Epoch";

    public const string ContainerName = nameof(ContainerName);
    public const string OutputFormat = nameof(OutputFormat);
    public const string IsStreamCacheEnabled = nameof(IsStreamCacheEnabled);
    public const string StreamCacheConnectionString = nameof(StreamCacheConnectionString);
    public const string Schedule = nameof(Schedule);
    public const string UseCurrentTimeForExport = nameof(UseCurrentTimeForExport);
    public const string FileNamePattern = nameof(FileNamePattern);
    public const string ShouldWriteGuidAsString = nameof(ShouldWriteGuidAsString);
    public const string ShouldEscapeVocabularyKeys = nameof(ShouldEscapeVocabularyKeys);
    public const string CustomCron = nameof(CustomCron);
    public const string IsDeltaMode = nameof(IsDeltaMode);
    public const string IsSoftDelete = nameof(IsSoftDelete);
    public const string IsOverwriteEnabled = nameof(IsOverwriteEnabled);
    public const string IsArrayColumnsEnabled = nameof(IsArrayColumnsEnabled);
    public const string UseLegacyOptions = nameof(UseLegacyOptions);

    public const string IdKey = "Id";
    public const string StreamCacheConnectionStringKey = "StreamCache";
    public const string CustomCronScheduleName = "CustomCron";

    internal static class OutputFormats
    {
        private static readonly HashSet<string> _allSupportedFormats = new(StringComparer.OrdinalIgnoreCase)
        {
            Csv,
            Json,
            Parquet,
        };
        private static readonly HashSet<string> _reducedSupportedFormats = new(StringComparer.OrdinalIgnoreCase)
        {
            Csv,
            Parquet,
        };

        public const string Csv = "CSV";
        public const string Json = "JSON";
        public const string Parquet = "Parquet";

        public static ICollection<string> AllSupportedFormats => _allSupportedFormats;

        public static ICollection<string> ReducedSupportedFormats => _reducedSupportedFormats;

        public static bool IsValid(string format, bool isReducedSupportedFormat = false)
        {
            return isReducedSupportedFormat
                ? _reducedSupportedFormats.Contains(format)
                : _allSupportedFormats.Contains(format);
        }
    }

    protected StorageConfigurationConstants(
        Guid providerId,
        string providerName,
        string componentName,
        string icon,
        string domain,
        string about,
        AuthMethods authMethods,
        string guideDetails,
        IEnumerable<Control> properties = null,
        IntegrationType integrationType = IntegrationType.Connector,
        string guideInstructions = "Provide authentication instructions here, if applicable",
        string featureCategory = "Connectivity",
        string featureDescription = "Expenses and Invoices against customers")
        : base(providerId, providerName, componentName, icon, domain, about, authMethods, guideDetails, properties, integrationType, guideInstructions, featureCategory, featureDescription)
    {
    }

    /// <summary>
    /// Environment key name for cache sync interval
    /// </summary>
    public string CacheSyncIntervalKeyName => $"Streams.{CacheKeyword}.CacheSyncInterval";

    /// <summary>
    /// Default value for Cache sync interval in milliseconds
    /// </summary>
    public int CacheSyncIntervalDefaultValue => 60_000;

    /// <summary>
    /// Environment key name for cache records threshold
    /// </summary>
    public string CacheRecordsThresholdKeyName => $"Streams.{CacheKeyword}.CacheRecordsThreshold";

    /// <summary>
    /// Default value for Cache records threshold
    /// </summary>
    public int CacheRecordsThresholdDefaultValue => 50;

    /// <summary>
    /// Environment key name for the minimum interval between repeated error logs from health-check connection verification.
    /// </summary>
    public string HealthCheckErrorLogIntervalKeyName => $"Streams.{CacheKeyword}.HealthCheckErrorLogInterval";

    /// <summary>
    /// Default value for health-check error log interval in milliseconds.
    /// </summary>
    public int HealthCheckErrorLogIntervalDefaultValue => 60 * 60 * 1000; // 1 hour in milliseconds

    public string CacheBufferStrategyKeyName => $"Streams.{CacheKeyword}.CacheBufferStrategy";

    public string CacheBufferStrategyDefaultValue => nameof(BufferStrategy.Safe);

    public string EnableCustomCronKeyName => $"Streams.{CacheKeyword}.Scheduling.CustomCron.Enabled";

    public bool EnableCustomCronDefaultValue => false;

    protected abstract string CacheKeyword { get; }

    /// <summary>
    /// Shape of the export option controls. The same options are shown on the connector while legacy options are used
    /// and on every stream (as connector properties) otherwise.
    /// </summary>
    protected readonly record struct ExportOptionsSettings(
        bool IsCustomFileNamePatternSupported = true,
        bool IsArrayColumnOptionEnabled = false,
        bool IsReducedFormats = false,
        bool IsDeltaOptionEnabled = false,
        bool IsForceStreamCache = false,
        bool IsOutputFormatHidden = false,
        bool IsEscapeVocabularyKeysOptionEnabled = false,
        string EscapeVocabularyKeysHelp = DefaultEscapeVocabularyKeysHelp,
        bool IsWriteGuidAsStringOptionEnabled = false);

    private const string DefaultEscapeVocabularyKeysHelp = """
        Replaces characters in the column names that are not in this list ('a-z', 'A-Z', '0-9' and '_') with the character '_'.
        Enable this if you plan to access the output file in Microsoft Purview.
        """;

    /// <summary>
    /// Keys of the options that are configured on the connector while legacy options are used, and on the stream otherwise.
    /// </summary>
    internal static readonly IReadOnlySet<string> ExportOptionKeys = new HashSet<string>
    {
        IsStreamCacheEnabled,
        OutputFormat,
        Schedule,
        CustomCron,
        FileNamePattern,
        IsArrayColumnsEnabled,
        IsDeltaMode,
        ShouldEscapeVocabularyKeys,
        ShouldWriteGuidAsString,
    };

    /// <summary>
    /// Legacy options are used unless <see cref="UseLegacyOptions"/> is explicitly set to false.
    /// </summary>
    internal static bool IsUsingLegacyOptions(IDictionary<string, object> configurations)
    {
        return !configurations.TryGetValue(UseLegacyOptions, out var value)
            || value switch
            {
                bool useLegacyOptions => useLegacyOptions,
                string text when bool.TryParse(text, out var useLegacyOptions) => useLegacyOptions,
                _ => true,
            };
    }

    /// <summary>
    /// Makes the controls only visible while legacy options are used, i.e. when they are not configured on the stream.
    /// </summary>
    protected static void RequireLegacyOptions(IEnumerable<Control> controls)
    {
        foreach (var control in controls)
        {
            control.DisplayDependencies =
            [
                .. control.DisplayDependencies,
                new ControlDisplayDependency
                {
                    Name = UseLegacyOptions,
                    Operator = ControlDependencyOperator.NotEquals,
                    Value = false.ToString().ToLowerInvariant(),
                    UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                },
            ];
        }
    }

    /// <summary>
    /// Creates the controls for the connector's authentication section: the "Use Legacy Options" switch,
    /// the stream cache connection string and the export options, which are only visible while legacy options are used.
    /// </summary>
    protected static List<Control> GetAuthMethods(ApplicationContext applicationContext, ExportOptionsSettings settings)
    {
        var controls = new List<Control>
        {
            new()
            {
                Name = UseLegacyOptions,
                DisplayName = "Use Legacy Options",
                Type = "checkbox",
                IsRequired = false,
                Help = """
                    When enabled, export options (e.g. output format and schedule) are configured here and apply to all streams.
                    When disabled, they are configured on each stream instead.
                    """,
                Options = new Dictionary<string, object> { ["defaultValue"] = true },
            },
        };

        var exportOptions = GetExportOptions(settings);
        RequireLegacyOptions(exportOptions);

        controls.AddRange(exportOptions);

        string connectionString = null;
        if (applicationContext.System.ConnectionStrings.ConnectionStringExists(StreamCacheConnectionStringKey))
        {
            connectionString = applicationContext.System.ConnectionStrings.GetConnectionString(StreamCacheConnectionStringKey);
        }

        if (!settings.IsForceStreamCache && string.IsNullOrWhiteSpace(connectionString))
        {
            // The connection string is a credential, so it stays on the connector and is not gated by legacy options
            controls.Insert(
                controls.FindIndex(control => control.Name == Schedule),
                new()
                {
                    Name = StreamCacheConnectionString,
                    DisplayName = "Stream Cache Connection String (SQL Server)",
                    Type = "password",
                    IsRequired = false,
                });
        }

        return controls;
    }

    /// <summary>
    /// Creates the export option controls. Each call returns new instances.
    /// </summary>
    protected static List<Control> GetExportOptions(ExportOptionsSettings settings)
    {
        var (isCustomFileNamePatternSupported, isArrayColumnOptionEnabled, isReducedFormats, isDeltaOptionEnabled, isForceStreamCache, isOutputFormatHidden, isEscapeVocabularyKeysOptionEnabled, escapeVocabularyKeysHelp, isWriteGuidAsStringOptionEnabled) = settings;

        var utcExportHelpText = $"Files will be exported using {TimeZoneInfo.Utc.Id} time zone (offset {TimeZoneInfo.Utc.BaseUtcOffset:hh\\:mm})";

        var controls = new List<Control>();
        if (!isForceStreamCache)
        {
            controls.Add(new()
            {
                Name = IsStreamCacheEnabled,
                DisplayName = "Enable Stream Cache (Sync mode only)",
                Type = "checkbox",
                IsRequired = false,
                DisplayDependencies =
                [
                    new ControlDisplayDependency
                    {
                        Name = "__StreamProperty__StreamMode",
                        Operator = ControlDependencyOperator.Equals,
                        Value = nameof(StreamMode.Sync),
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    }
                ],
            });
        };

        var streamCacheDependency = new ControlDisplayDependency
        {
            Name = IsStreamCacheEnabled,
            Operator = ControlDependencyOperator.Exists,
            UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
        };

        if (!isOutputFormatHidden)
        {
            controls.Add(new()
            {
                Name = OutputFormat,
                DisplayName = "Output Format",
                Type = "option",
                IsRequired = true,
                SourceType = ControlSourceType.Dynamic,
                Source = isReducedFormats
                        ? FileStorageExtendedConfigurationProvider.ReducedFormatsSourceName
                        : FileStorageExtendedConfigurationProvider.DefaultSourceName,
                DisplayDependencies = isForceStreamCache ? [] : [streamCacheDependency],
            });
        }

        controls.Add(
            new ()
            {
                Name = Schedule,
                DisplayName = "Export Schedule",
                Type = "option",
                Help = utcExportHelpText,
                IsRequired = true,
                SourceType = ControlSourceType.Dynamic,
                Source = FileStorageExtendedConfigurationProvider.DefaultSourceName,
                DisplayDependencies = isForceStreamCache ? [] : [streamCacheDependency],
            });

        var customScheduleDependency = new ControlDisplayDependency
        {
            Name = Schedule,
            Operator = ControlDependencyOperator.Equals,
            Value = CustomCronScheduleName,
            UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
        };

        controls.Add(
            new()
            {
                Name = CustomCron,
                DisplayName = "Custom Cron",
                Type = "input",
                Help = utcExportHelpText,
                IsRequired = true,
                DataDependencies = new[]
                {
                    new ControlDataDependency
                    {
                        Name = Schedule,
                    },
                },
                DisplayDependencies = isForceStreamCache
                    ? [ customScheduleDependency ]
                    : [ streamCacheDependency, customScheduleDependency ],
            });

        if (isCustomFileNamePatternSupported)
        {
            controls.Add(
                new()
                {
                    Name = FileNamePattern,
                    DisplayName = "File Name Pattern",
                    Type = "input",
                    Help = """
                       Specify a file name pattern for the export file, e.g. {StreamId}_{DataTime}.{OutputFormat}.
                       Available variables are {StreamId}, {DataTime}, {OutputFormat} and {ContainerName}.
                       Variables can also be formatted using formatString modifier. For more information, please refer to the documentation.
                       """,
                    IsRequired = false,
                    DisplayDependencies = isForceStreamCache ? [] : [ streamCacheDependency ],
                });
        }

        var parquetFormatDependency = new ControlDisplayDependency
        {
            Name = OutputFormat,
            Operator = ControlDependencyOperator.Equals,
            Value = OutputFormats.Parquet.ToLowerInvariant(),
            UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
        };
        if (isArrayColumnOptionEnabled)
        {
            controls.Add(
                new()
                {
                    Name = IsArrayColumnsEnabled,
                    DisplayName = "Exports Codes and Edges in array format",
                    Type = "checkbox",
                    IsRequired = false,
                    DisplayDependencies = isForceStreamCache
                    ? [ parquetFormatDependency ]
                    : [ streamCacheDependency, parquetFormatDependency ],
                });
        }
        if (isDeltaOptionEnabled)
        {
            controls.Add(
                new()
                {
                    Name = IsDeltaMode,
                    DisplayName = "Delta Mode",
                    Type = "checkbox",
                    Help = """
                       Only write changes since last export instead of all data
                       """,
                    IsRequired = false,
                    DisplayDependencies = isForceStreamCache
                    ? [ parquetFormatDependency ]
                    : [ streamCacheDependency, parquetFormatDependency],
                });
        }
        if (isEscapeVocabularyKeysOptionEnabled)
        {
            controls.Add(
                new()
                {
                    Name = ShouldEscapeVocabularyKeys,
                    DisplayName = "Replace Non-Alphanumeric Characters in Column Names",
                    Type = "checkbox",
                    IsRequired = false,
                    Help = escapeVocabularyKeysHelp,
                    DisplayDependencies = isForceStreamCache ? [] : [ streamCacheDependency ],
                });
        }
        if (isWriteGuidAsStringOptionEnabled)
        {
            controls.Add(
                new()
                {
                    Name = ShouldWriteGuidAsString,
                    DisplayName = "Write Guid as string",
                    Type = "checkbox",
                    IsRequired = false,
                    Help = """
                        Write Guid values as string instead of byte array.
                        Enable this if you plan to access the output file in Microsoft Purview.
                        """,
                    DisplayDependencies = [ parquetFormatDependency ],
                });
        }
        return controls;
    }
}
