using System;
using System.Collections.Generic;

using CluedIn.Core;
using CluedIn.Core.Providers;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.DataLake.Common;

public abstract class DataLakeConstants : ConfigurationConstantsBase, IDataLakeConstants
{
    public const string OutputFormat = nameof(OutputFormat);
    public const string IsStreamCacheEnabled = nameof(IsStreamCacheEnabled);
    public const string StreamCacheConnectionString = nameof(StreamCacheConnectionString);
    public const string Schedule = nameof(Schedule);
    public const string UseCurrentTimeForExport = nameof(UseCurrentTimeForExport);
    public const string FileNamePattern = nameof(FileNamePattern);
    public const string ShouldWriteGuidAsString = nameof(ShouldWriteGuidAsString);
    public const string ShouldEscapeVocabularyKeys = nameof(ShouldEscapeVocabularyKeys);

    public const string IdKey = "Id";
    public const string StreamCacheConnectionStringKey = "StreamCache";


    internal static class OutputFormats
    {
        private static readonly HashSet<string> _SupportedFormats = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            Csv,
            Json,
            Parquet,
        };

        public const string Csv = "csv";
        public const string Json = "json";
        public const string Parquet = "parquet";

        public static ICollection<string> SupportedFormats => _SupportedFormats;
        public static bool IsValid(string format)
        {
            return _SupportedFormats.Contains(format);
        }
    }

    internal static class JobScheduleNames
    {
        private static readonly HashSet<string> _SupportedSchedules = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            Hourly,
            Daily,
            Weekly,
        };

        public const string Hourly = "Hourly";
        public const string Daily = "Daily";
        public const string Weekly = "Weekly";
        public const string Never = "Never";

        public static ICollection<string> SupportedSchedules => _SupportedSchedules;

        public static bool IsValid(string schedule)
        {
            return _SupportedSchedules.Contains(schedule);
        }
    }

    internal static readonly Dictionary<string, string> CronSchedules = new(StringComparer.OrdinalIgnoreCase)
    {
        [JobScheduleNames.Hourly] = "0 0/1 * * *",
        [JobScheduleNames.Daily] = "0 0 1-31 * *",
        [JobScheduleNames.Weekly] = "0 0 1-31 * 1",
        [JobScheduleNames.Never] = "0 5 31 2 *",
    };

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

    protected abstract string CacheKeyword { get; }

    protected static IEnumerable<Control> GetAuthMethods(ApplicationContext applicationContext)
    {
        string connectionString = null;
        if (applicationContext.System.ConnectionStrings.ConnectionStringExists(StreamCacheConnectionStringKey))
        {
            connectionString = applicationContext.System.ConnectionStrings.GetConnectionString(StreamCacheConnectionStringKey);
        }
        var controls = new List<Control>
        {
            new ()
            {
                name = IsStreamCacheEnabled,
                displayName = "Enable Stream Cache (Sync mode only)",
                type = "checkbox",
                isRequired = false,
            },
            new ()
            {
                name = OutputFormat,
                displayName = "Output Format. JSON/Parquet/CSV (Parquet & CSV only when stream cache is enabled)",
                type = "input",
                isRequired = true,
            },
        };

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            controls.Add(
                new ()
                {
                    name = StreamCacheConnectionString,
                    displayName = "Stream Cache Connection String (SQL Server)",
                    type = "password",
                    isRequired = false,
                });
        }

        controls.Add(
            new ()
            {
                name = Schedule,
                displayName = $"Schedule (Hourly/Daily/Weekly)",
                type = "input",
                isRequired = true,
            });

        controls.Add(
            new()
            {
                name = FileNamePattern,
                displayName = "File Name Pattern",
                type = "input",
                isRequired = false,
            });

        return controls;
    }
}
