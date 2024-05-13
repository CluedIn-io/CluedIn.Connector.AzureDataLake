using CluedIn.Core;
using CluedIn.Core.Providers;
using System;
using System.Collections.Generic;
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
        public const string IsStreamCacheEnabled = nameof(IsStreamCacheEnabled);
        public const string StreamCacheConnectionString = nameof(StreamCacheConnectionString);
        public const string Schedule = nameof(Schedule);

        public const string IdKey = "Id";


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

        public AzureDataLakeConstants(ApplicationContext applicationContext) : base(Guid.Parse("F6178E19-7168-449C-B4B6-F9810E86C1C2"),
            providerName: "Azure DataLake Connector",
            componentName: "AzureDataLakeConnector",
            icon: "Resources.azuredatalake.svg",
            domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
            about: "Supports publishing of data to Azure Data Lake Storage Gen2.",
            authMethods: GetAzureDataLakeAuthMethods(applicationContext),
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

        private static AuthMethods GetAzureDataLakeAuthMethods(ApplicationContext applicationContext)
        {
            const string StreamCacheConnectionKey = "StreamCache";
            string connectionString = null;
            if (applicationContext.System.ConnectionStrings.ConnectionStringExists(StreamCacheConnectionKey))
            {
                connectionString = applicationContext.System.ConnectionStrings.GetConnectionString(StreamCacheConnectionKey);
            }
            var controls = new List<Control>
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
                    name = IsStreamCacheEnabled,
                    displayName = "Enable Stream Cache",
                    type = "checkbox",
                    isRequired = false,
                },
                new Control
                {
                    name = OutputFormat,
                    displayName = "JSON (Stream Cache disabled), JSON/Parquet/CSV (Stream cache enabled)",
                    type = "input",
                    isRequired = true,
                },
            };

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                controls.Add(
                    new Control
                    {
                        name = StreamCacheConnectionString,
                        displayName = "Stream Cache Connection String (SQL Server)",
                        type = "password",
                        isRequired = false,
                    });
            }
            controls.Add(
                new Control
                {
                    name = Schedule,
                    displayName = $"Schedule (Hourly/Daily/Weekly)",
                    type = "input",
                    isRequired = true,
                });
            return new AuthMethods
            {
                token = controls
            };
        }
    }
}
