using System;
using System.Collections.Generic;

using CluedIn.Core;
using CluedIn.Core.Providers;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.DataLake.Common;

public abstract class DataLakeConstants : ConfigurationConstantsBase, IDataLakeConstants
{
    internal const string ProviderDefinitionIdKey = "__ProviderDefinitionId__";
    internal const string ChangeTypeKey = "__ChangeType__";

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
    public const string IsOverwriteEnabled = nameof(IsOverwriteEnabled);
    public const string IsSerializedArrayColumnsEnabled = nameof(IsSerializedArrayColumnsEnabled);

    public const string IdKey = "Id";
    public const string StreamCacheConnectionStringKey = "StreamCache";
    public const string CustomCronScheduleName = "CustomCron";

    internal static class OutputFormats
    {
        private static readonly HashSet<string> _supportedFormats = new(StringComparer.OrdinalIgnoreCase)
        {
            Csv,
            Json,
            Parquet,
        };

        public const string Csv = "CSV";
        public const string Json = "JSON";
        public const string Parquet = "Parquet";

        public static ICollection<string> SupportedFormats => _supportedFormats;
        public static bool IsValid(string format)
        {
            return _supportedFormats.Contains(format);
        }
    }

    protected DataLakeConstants(
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

    public string EnableCustomCronKeyName => $"Streams.{CacheKeyword}.Scheduling.CustomCron.Enabled";

    public bool EnableCustomCronDefaultValue => false;

    protected abstract string CacheKeyword { get; }

    protected static IEnumerable<Control> GetAuthMethods(
        ApplicationContext applicationContext,
        bool isCustomFileNamePatternSupported = true,
        bool isDeltaOptionEnabled = false,
        bool isSerializedArrayColumnEnabled = false)
    {
        string connectionString = null;
        if (applicationContext.System.ConnectionStrings.ConnectionStringExists(StreamCacheConnectionStringKey))
        {
            connectionString = applicationContext.System.ConnectionStrings.GetConnectionString(StreamCacheConnectionStringKey);
        }

        var utcExportHelpText = $"Files will be exported using {TimeZoneInfo.Utc.Id} time zone (offset {TimeZoneInfo.Utc.BaseUtcOffset:hh\\:mm})";
        var controls = new List<Control>
        {
            new ()
            {
                Name = IsStreamCacheEnabled,
                DisplayName = "Enable Stream Cache (Sync mode only)",
                Type = "checkbox",
                IsRequired = false,
            },
            new ()
            {
                Name = OutputFormat,
                DisplayName = "Output Format",
                Type = "option",
                IsRequired = true,
                SourceType = ControlSourceType.Dynamic,
                Source = DataLakeExtendedConfigurationProvider.SourceName,
                DisplayDependencies = new[]
                {
                    new ControlDisplayDependency
                    {
                        Name = IsStreamCacheEnabled,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                },
            },
        };

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            controls.Add(
                new ()
                {
                    Name = StreamCacheConnectionString,
                    DisplayName = "Stream Cache Connection String (SQL Server)",
                    Type = "password",
                    IsRequired = false,
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
                Source = DataLakeExtendedConfigurationProvider.SourceName,
                DisplayDependencies = new[]
                {
                    new ControlDisplayDependency
                    {
                        Name = IsStreamCacheEnabled,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                },
            });
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
                DisplayDependencies = new[]
                {
                    new ControlDisplayDependency
                    {
                        Name = IsStreamCacheEnabled,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                    new ControlDisplayDependency
                    {
                        Name = Schedule,
                        Operator = ControlDependencyOperator.Equals,
                        Value = CustomCronScheduleName,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                },
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
                    DisplayDependencies = new[]
                    {
                    new ControlDisplayDependency
                    {
                        Name = IsStreamCacheEnabled,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                    },
                });
        }
        if (isSerializedArrayColumnEnabled)
        {
            controls.Add(
                new()
                {
                    Name = IsSerializedArrayColumnsEnabled,
                    DisplayName = "Adds additional column for Codes and Edges in string format",
                    Type = "checkbox",
                    IsRequired = false,
                    DisplayDependencies = new[]
                    {
                    new ControlDisplayDependency
                    {
                        Name = IsStreamCacheEnabled,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                    new ControlDisplayDependency
                    {
                        Name = OutputFormat,
                        Operator = ControlDependencyOperator.Equals,
                        Value = OutputFormats.Parquet,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                    },
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
                       Write changes since last export instead of all data
                       """,
                    IsRequired = false,
                    DisplayDependencies = new[]
                    {
                    new ControlDisplayDependency
                    {
                        Name = IsStreamCacheEnabled,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                    },
                });
        }
        return controls;
    }
}
