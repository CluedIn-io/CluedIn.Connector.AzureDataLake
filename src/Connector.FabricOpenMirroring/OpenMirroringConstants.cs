using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.FabricOpenMirroring;
public class OpenMirroringConstants : DataLakeConstants, IOpenMirroringConstants
{
    internal static readonly Guid DataLakeProviderId = Guid.Parse("75CD5880-0537-4C6D-AE14-511C273ACD68");

    public const string WorkspaceName = nameof(WorkspaceName);
    public const string MirroredDatabaseName = nameof(MirroredDatabaseName);
    public const string ClientId = nameof(ClientId);
    public const string ClientSecret = nameof(ClientSecret);
    public const string TenantId = nameof(TenantId);
    public const string ShouldCreateMirroredDatabase = nameof(ShouldCreateMirroredDatabase);

    public OpenMirroringConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
        providerName: "Fabric Open Mirroring Connector",
        componentName: "FabricOpenMirroringConnector",
        icon: "Resources.fabricOpenMirroring.svg",
        domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
        about: "Supports publishing of data to Microsoft Fabric Mirrored Database using Open Mirroring.",
        authMethods: GetOpenMirroringAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to Microsoft Fabric Mirrored Database using Open Mirroring.",
        guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
    {
    }

    protected override string CacheKeyword => "FabricOpenMirroringConnector";

    private static AuthMethods GetOpenMirroringAuthMethods(ApplicationContext applicationContext)
    {
        var controls = new List<Control>
        {
            new ()
            {
                Name = WorkspaceName,
                DisplayName = "Workspace Name",
                Type = "input",
                IsRequired = true,
            },
            new ()
            {
                Name = ShouldCreateMirroredDatabase,
                DisplayName = "Create Mirrored Database",
                Type = "checkbox",
                IsRequired = true,
                Help = """
                Automatically creates the Mirrored Database if it doesn't exist. Requires 'Admin' permission in the workspace.
                """,
            },
            new ()
            {
                Name = MirroredDatabaseName,
                DisplayName = "Mirrored Database Name",
                Type = "input",
                IsRequired = false,
                Help = "When left blank, one will be created with the name 'CluedIn_ExportTarget_{ExportTargetId}'"
            },
            new ()
            {
                Name = ClientId,
                DisplayName = "Client Id",
                Type = "input",
                IsRequired = true,
                Help = """
                'Contributor' permission is required in the workspace when using existing Mirrored Database.
                'Admin' permission is required in the workspace when Mirrored Database should be created automatically.
                """,
            },
            new ()
            {
                Name = ClientSecret,
                DisplayName = "Client Secret",
                Type = "password",
                IsRequired = true,
            },
            new ()
            {
                Name = TenantId,
                DisplayName = "Tenant Id",
                Type = "input",
                IsRequired = true,
            },
        };

        controls.AddRange(
            GetAuthMethods(
                applicationContext,
                isCustomFileNamePatternSupported: false,
                isReducedFormats: true,
                isArrayColumnOptionEnabled: false));

        return new AuthMethods
        {
            Token = controls
        };
    }
}
