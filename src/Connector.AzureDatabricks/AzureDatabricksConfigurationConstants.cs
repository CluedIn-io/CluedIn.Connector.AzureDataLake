using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.AzureDatabricks;

public class AzureDatabricksConfigurationConstants : StorageConfigurationConstants, IAzureDatabricksConfigurationConstants
{
    internal static readonly Guid FileStorageProviderId = Guid.Parse("615F15FD-91A2-4B8A-9CE5-FEF4F7C44ACE");

    public const string WorkspaceName = nameof(WorkspaceName);
    public const string ItemName = nameof(ItemName);
    public const string ItemType = nameof(ItemType);
    public const string ItemFolder = nameof(ItemFolder);
    public const string ClientId = nameof(ClientId);
    public const string ClientSecret = nameof(ClientSecret);
    public const string TenantId = nameof(TenantId);

    public AzureDatabricksConfigurationConstants(ApplicationContext applicationContext) : base(FileStorageProviderId,
        providerName: "Azure Databricks Connector",
        componentName: "AzureDatabricksConnector",
        icon: "Resources.azuredatabricks.svg",
        domain: "https://azure.microsoft.com/en-au/products/databricks",
        about: "Supports publishing of data to Azure Databricks via OneLake.",
        authMethods: GetAzureFileStorageAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to Azure Databricks.",
        guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
    {
    }

    protected override string CacheKeyword => "AzureDatabricksConnector";

    private static AuthMethods GetAzureFileStorageAuthMethods(ApplicationContext applicationContext)
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
