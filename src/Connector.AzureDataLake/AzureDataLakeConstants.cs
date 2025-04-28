using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.AzureDataLake;

public class AzureDataLakeConstants : DataLakeConstants, IAzureDataLakeConstants
{
    internal static readonly Guid DataLakeProviderId = Guid.Parse("F6178E19-7168-449C-B4B6-F9810E86C1C2");

    public const string AccountName = nameof(AccountName);
    public const string AccountKey = nameof(AccountKey);
    public const string FileSystemName = nameof(FileSystemName);
    public const string DirectoryName = nameof(DirectoryName);

     public AzureDataLakeConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
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

    protected override string CacheKeyword => "AzureDataLakeConnector";

    private static AuthMethods GetAzureDataLakeAuthMethods(ApplicationContext applicationContext)
    {
        var controls = new List<Control>
        {
            new ()
            {
                Name = AccountName,
                DisplayName = AccountName,
                Type = "input",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
            },
            new ()
            {
                Name = AccountKey,
                DisplayName = AccountKey,
                Type = "password",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
            },
            new ()
            {
                Name = FileSystemName,
                DisplayName = FileSystemName,
                Type = "input",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
            },
            new ()
            {
                Name = DirectoryName,
                DisplayName = DirectoryName,
                Type = "input",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
            },
        };

        controls.AddRange(GetAuthMethods(applicationContext, isArrayColumnOptionEnabled: true));
        controls.Add(
            new()
            {
                Name = ShouldEscapeVocabularyKeys,
                DisplayName = "Replace Non-Alphanumeric Characters in Column Names",
                Type = "checkbox",
                IsRequired = false,
                Help = """
                    Replaces characters in the column names that are not in this list ('a-z', 'A-Z', '0-9' and '_') with the character '_'.
                    Enable this if you plan to access the output file in Microsoft Purview.
                    """,
                DisplayDependencies = new[]
                {
                    new ControlDisplayDependency
                    {
                        Name = IsStreamCacheEnabled,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                },
            }
        );
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
                DisplayDependencies = new[]
                {
                    new ControlDisplayDependency
                    {
                        Name = OutputFormat,
                        Operator = ControlDependencyOperator.Equals,
                        Value = OutputFormats.Parquet.ToLowerInvariant(),
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                },
            }
        );

        return new AuthMethods
        {
            Token = controls
        };
    }
}
