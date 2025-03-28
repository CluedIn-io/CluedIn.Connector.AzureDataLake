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
    public const string MirroredDatabaseName = nameof(MirroredDatabaseName);
    public const string ClientId = nameof(ClientId);
    public const string ClientSecret = nameof(ClientSecret);
    public const string TenantId = nameof(TenantId);
    public const string ShouldCreateMirroredDatabase = nameof(ShouldCreateMirroredDatabase);

    public FabricMirroringConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
        providerName: "Fabric Mirroring Connector",
        componentName: "FabricMirroringConnector",
        icon: "Resources.fabricMirroring.svg",
        domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
        about: "Supports publishing of data to Microsoft Fabric Mirrored Database.",
        authMethods: GetFabricMirroringAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to Microsoft Fabric Mirrored Database.",
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
                Name = MirroredDatabaseName,
                DisplayName = MirroredDatabaseName,
                Type = "input",
                IsRequired = false,
                Help = "When left blank, one will be auto created"
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

        controls.AddRange(GetAuthMethods(applicationContext, isCustomFileNamePatternSupported: false));

        return new AuthMethods
        {
            Token = controls
        };
    }
}
