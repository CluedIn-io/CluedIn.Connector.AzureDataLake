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

    public const string AccountName = nameof(AccountName);
    public const string AccountKey = nameof(AccountKey);
    public const string FileSystemName = nameof(FileSystemName);
    public const string DirectoryName = nameof(DirectoryName);

     public SynapseDataEngineeringConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
        providerName: "Synapse Data Engineering Connector",
        componentName: "SynapseDataEngineeringConnector",
        icon: "Resources.synapsedataengineering.svg",
        domain: "https://www.microsoft.com/en-us/microsoft-fabric",
        about: "Supports publishing of data to Synapse Data Engineering via its support for ADLSG2.",
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
                name = AccountName,
                displayName = AccountName,
                type = "input",
                isRequired = true
            },
            new ()
            {
                name = AccountKey,
                displayName = AccountKey,
                type = "password",
                isRequired = true
            },
            new ()
            {
                name = FileSystemName,
                displayName = FileSystemName,
                type = "input",
                isRequired = true
            },
            new ()
            {
                name = DirectoryName,
                displayName = DirectoryName,
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
