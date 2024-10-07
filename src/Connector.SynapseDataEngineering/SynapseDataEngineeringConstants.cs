using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.SynapseDataEngineering;

public class SynapseDataEngineeringConstants : DataLakeConstants, ISynapseDataEngineeringConstants
{
    internal static readonly Guid DataLakeProviderId = Guid.Parse("8246B581-43FE-4E9F-A411-F637BE89730A");

    public const string WorkspaceName = nameof(WorkspaceName);
    public const string ItemName = nameof(ItemName);
    public const string ItemType = nameof(ItemType);
    public const string ItemFolder = nameof(ItemFolder);
    public const string ClientId = nameof(ClientId);
    public const string ClientSecret = nameof(ClientSecret);
    public const string TenantId = nameof(TenantId);

    public SynapseDataEngineeringConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
        providerName: "Synapse Data Engineering Connector",
        componentName: "SynapseDataEngineeringConnector",
        icon: "Resources.synapsedataengineering.svg",
        domain: "https://www.microsoft.com/en-us/microsoft-fabric",
        about: "Supports publishing of data to Synapse Data Engineering via OneLake.",
        authMethods: GetAzureDataLakeAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to Synapse Data Engineering.",
        guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
    {
    }

    protected override string CacheKeyword => "SynapseDataEngineeringConnector";

    private static AuthMethods GetAzureDataLakeAuthMethods(ApplicationContext applicationContext)
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
