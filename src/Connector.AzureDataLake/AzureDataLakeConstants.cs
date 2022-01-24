using CluedIn.Connector.Common;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Core;
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

        public AzureDataLakeConstants() : base(Guid.Parse("F6178E19-7168-449C-B4B6-F9810E86C1C2"),
            providerName: "Azure DataLake Connector",
            componentName: "AzureDataLakeConnector",
            icon: "Resources.azuredatalake.png",
            domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
            about: "Supports publishing of data to Azure Data Lake Storage Gen2.",
            authMethods: AzureDataLaleAuthMethods,
            guideDetails: "Supports publishing of data to Azure DataLake.",
            guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
        {
        }

        private static AuthMethods AzureDataLaleAuthMethods => new AuthMethods
        {
            token = new[]
            {
                new Control
                {
                    name = AccountName.ToCamelCase(),
                    displayName = "Account Name",
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = AccountKey.ToCamelCase(),
                    displayName = "Account Key",
                    type = "password",
                    isRequired = true
                },
                new Control
                {
                    name = FileSystemName.ToCamelCase(),
                    displayName ="File System Name",
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = DirectoryName.ToCamelCase(),
                    displayName ="Directory Name",
                    type = "input",
                    isRequired = true
                }
            }
        };
    }
}
