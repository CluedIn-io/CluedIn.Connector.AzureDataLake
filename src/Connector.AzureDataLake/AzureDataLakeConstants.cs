using CluedIn.Core.Providers;
using System;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.AzureDataLake
{
    public class AzureDataLakeConstants : ConfigurationConstantsBase, IAzureDataLakeConstants
    {
        public const string AccountName = nameof(AccountName);
        public const string AccountKey = nameof(AccountKey);
        public const string FileSystemName = nameof(FileSystemName);
        public const string DirectoryName = nameof(DirectoryName);
        public const string OutputFormat = nameof(OutputFormat);
        public const string EnableBuffer = nameof(EnableBuffer);
        public const string BufferConnectionString = nameof(BufferConnectionString);
        public const string Schedule = nameof(Schedule);

        public static class OutputFormats
        {
            public const string Csv = "csv";
            public const string Json = "json";
            public const string Parquet = "parquet";
        }
        public static class JobScheduleNames
        {
            public const string Hourly = "Hourly";
            public const string Daily = "Daily";
            public const string Weekly = "Weekly";
            public const string Never = "Never";
        }
        public AzureDataLakeConstants() : base(Guid.Parse("F6178E19-7168-449C-B4B6-F9810E86C1C2"),
            providerName: "Azure DataLake Connector",
            componentName: "AzureDataLakeConnector",
            icon: "Resources.azuredatalake.svg",
            domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
            about: "Supports publishing of data to Azure Data Lake Storage Gen2.",
            authMethods: AzureDataLakeAuthMethods,
            guideDetails: "Supports publishing of data to Azure DataLake.",
            guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
        {
        }

        /// <summary>
        /// Environment key name for cache sync interval
        /// </summary>
        public string CacheSyncIntervalKeyName => "Streams.AzureDataLakeConnector.CacheSyncInterval";

        /// <summary>
        /// Default value for Cache sync interval in milliseconds
        /// </summary>
        public int CacheSyncIntervalDefaultValue => 60_000;

        /// <summary>
        /// Environment key name for cache records threshold
        /// </summary>
        public string CacheRecordsThresholdKeyName => "Streams.AzureDataLakeConnector.CacheRecordsThreshold";

        /// <summary>
        /// Default value for Cache records threshold
        /// </summary>
        public int CacheRecordsThresholdDefaultValue => 50;

        private static AuthMethods AzureDataLakeAuthMethods => new AuthMethods
        {
            token = new[]
            {
                new Control
                {
                    name = AccountName,
                    displayName = AccountName,
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = AccountKey,
                    displayName = AccountKey,
                    type = "password",
                    isRequired = true
                },
                new Control
                {
                    name = FileSystemName,
                    displayName = FileSystemName,
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = DirectoryName,
                    displayName = DirectoryName,
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = OutputFormat,
                    displayName = "JSON/Parquet/CSV",
                    type = "input",
                    isRequired = true,
                },
                new Control
                {
                    name = EnableBuffer,
                    displayName = "Enable Buffer",
                    type = "checkbox",
                    isRequired = true,
                },
                new Control
                {
                    name = BufferConnectionString,
                    displayName = "Buffer Connection String (SQL Server)",
                    type = "password",
                    isRequired = true,
                },
                new Control
                {
                    name = Schedule,
                    displayName = $"Schedule (Hourly/Daily/Weekly)",
                    type = "input",
                    isRequired = true,
                },
            }
        };
    }
}
