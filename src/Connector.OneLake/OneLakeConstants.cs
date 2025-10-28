using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.OneLake;
public class OneLakeConstants : DataLakeConstants, IOneLakeConstants
{
    internal static readonly Guid DataLakeProviderId = Guid.Parse("36C1B087-97C0-4460-A813-6E4EA1D1BC9A");

    public const string WorkspaceName = nameof(WorkspaceName);
    public const string ItemName = nameof(ItemName);
    public const string ItemType = nameof(ItemType);
    public const string ItemFolder = nameof(ItemFolder);
    public const string ClientId = nameof(ClientId);
    public const string ClientSecret = nameof(ClientSecret);
    public const string TenantId = nameof(TenantId);
    public const string ShouldLoadToTable = nameof(ShouldLoadToTable);
    public const string TableName = nameof(TableName);

    public OneLakeConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
        providerName: "OneLake Connector",
        componentName: "OneLakeConnector",
        icon: "Resources.onelake.svg",
        domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
        about: "Supports publishing of data to OneLake.",
        authMethods: GetOneLakeAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to OneLake.",
        guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
    {
    }

    protected override string CacheKeyword => "OneLakeConnector";

    private static AuthMethods GetOneLakeAuthMethods(ApplicationContext applicationContext)
    {
        var controls = new List<Control>
        {
            new ()
            {
                Name = WorkspaceName,
                DisplayName = "Workspace Name",
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
                Name = ItemName,
                DisplayName = "Item Name",
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
                Name = ItemType,
                DisplayName = "Item Type",
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
                Name = ItemFolder,
                DisplayName = "Item Folder",
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
                Name = ClientId,
                DisplayName = "Client Id",
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
                Name = ClientSecret,
                DisplayName = "Client Secret",
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
                Name = TenantId,
                DisplayName = "Tenant Id",
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
                    Enable this if you plan to load the file to a table.
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
                Name = ShouldLoadToTable,
                DisplayName = "Load to table after exporting",
                Type = "checkbox",
                Help = """
                       Load data from file to table after exporting. Only applicable to CSV and Parquet export
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
                    new ControlDisplayDependency
                    {
                        Name = ShouldEscapeVocabularyKeys,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                },
            });
        controls.Add(
            new()
            {
                Name = TableName,
                DisplayName = "Table Name",
                Type = "input",
                Help = """
                       Table Name to load to after file export.
                       """,
                IsRequired = true,
                DisplayDependencies = new[]
                {
                    new ControlDisplayDependency
                    {
                        Name = ShouldLoadToTable,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                },
            });
        return new AuthMethods
        {
            Token = controls
        };
    }
}
