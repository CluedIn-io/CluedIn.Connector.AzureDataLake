using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.AzureAIStudio;
public class AzureAIStudioConstants : DataLakeConstants, IAzureAIStudioConstants
{
    internal static readonly Guid DataLakeProviderId = Guid.Parse("42BE3A28-FB10-47FA-935A-E06F83061C5B");

    public const string WorkspaceName = nameof(WorkspaceName);
    public const string ItemName = nameof(ItemName);
    public const string ItemType = nameof(ItemType);
    public const string ItemFolder = nameof(ItemFolder);
    public const string ClientId = nameof(ClientId);
    public const string ClientSecret = nameof(ClientSecret);
    public const string TenantId = nameof(TenantId);

    public AzureAIStudioConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
        providerName: "Azure AI Studio Connector",
        componentName: "AzureAIStudioConnector",
        icon: "Resources.azureaistudio.svg",
        domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
        about: "Supports publishing of data to Azure AI Studio via Onelake.",
        authMethods: GetOneLakeAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to Azure AI Studio.",
        guideInstructions: "Provide authentication instructions here, if applicable")
    {
    }

    protected override string CacheKeyword => "AzureAIStudioConnector";

    private static AuthMethods GetOneLakeAuthMethods(ApplicationContext applicationContext)
    {
        var controls = new List<Control>
        {
            new ()
            {
                name = WorkspaceName,
                displayName = WorkspaceName,
                type = "input",
                isRequired = true
            },
            new ()
            {
                name = ItemName,
                displayName = ItemName,
                type = "input",
                isRequired = true
            },
            new ()
            {
                name = ItemType,
                displayName = ItemType,
                type = "input",
                isRequired = true
            },
            new ()
            {
                name = ItemFolder,
                displayName = ItemFolder,
                type = "input",
                isRequired = true
            },
            new ()
            {
                name = ClientId,
                displayName = ClientId,
                type = "input",
                isRequired = true
            },
            new ()
            {
                name = ClientSecret,
                displayName = ClientSecret,
                type = "password",
                isRequired = true
            },
            new ()
            {
                name = TenantId,
                displayName = TenantId,
                type = "input",
                isRequired = true
            },
        };

        controls.AddRange(GetAuthMethods(applicationContext));

        return new AuthMethods
        {
            token = controls
        };
    }
}
