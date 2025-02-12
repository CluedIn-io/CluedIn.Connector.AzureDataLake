using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.FabricMirroring;
public class FabricMirroringConstants : DataLakeConstants, IFabricMirroringConstants
{
    internal static readonly Guid DataLakeProviderId = Guid.Parse("75CD5880-0537-4C6D-AE14-511C273ACD68");

    public const string WorkspaceName = nameof(WorkspaceName);
    public const string ItemName = nameof(ItemName);
    public const string ClientId = nameof(ClientId);
    public const string ClientSecret = nameof(ClientSecret);
    public const string TenantId = nameof(TenantId);
    public const string ShouldCreateMirroredDatabase = nameof(ShouldCreateMirroredDatabase);

    public FabricMirroringConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
        providerName: "FabricMirroring Connector",
        componentName: "FabricMirroringConnector",
        icon: "Resources.fabricMirroring.svg",
        domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
        about: "Supports publishing of data to FabricMirroring.",
        authMethods: GetFabricMirroringAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to FabricMirroring.",
        guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
    {
    }

    protected override string CacheKeyword => "FabricMirroringConnector";

    private static AuthMethods GetFabricMirroringAuthMethods(ApplicationContext applicationContext)
    {
        var controls = new List<Control>
        {
            new ()
            {
                Name = WorkspaceName,
                DisplayName = WorkspaceName,
                Type = "input",
                IsRequired = true,
            },
            new ()
            {
                Name = ItemName,
                DisplayName = ItemName,
                Type = "input",
                IsRequired = false,
            },
            new ()
            {
                Name = ClientId,
                DisplayName = ClientId,
                Type = "input",
                IsRequired = true,
            },
            new ()
            {
                Name = ClientSecret,
                DisplayName = ClientSecret,
                Type = "password",
                IsRequired = true,
            },
            new ()
            {
                Name = TenantId,
                DisplayName = TenantId,
                Type = "input",
                IsRequired = true,
            },
        };

        controls.AddRange(GetAuthMethods(applicationContext));

        return new AuthMethods
        {
            Token = controls
        };
    }
}
