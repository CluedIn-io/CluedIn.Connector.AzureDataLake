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
