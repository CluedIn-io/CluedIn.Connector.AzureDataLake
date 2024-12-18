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
                DisplayName = WorkspaceName,
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
                DisplayName = ItemName,
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
                DisplayName = ItemType,
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
                DisplayName = ItemFolder,
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
                DisplayName = ClientId,
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
                DisplayName = ClientSecret,
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
                DisplayName = TenantId,
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

        controls.AddRange(GetAuthMethods(applicationContext));

        return new AuthMethods
        {
            Token = controls
        };
    }
}
