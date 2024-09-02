using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.AzureDatabricks;

public class AzureDatabricksConstants : DataLakeConstants, IAzureDatabricksConstants
{
    internal static readonly Guid DataLakeProviderId = Guid.Parse("615F15FD-91A2-4B8A-9CE5-FEF4F7C44ACE");

    public const string AccountName = nameof(AccountName);
    public const string AccountKey = nameof(AccountKey);
    public const string FileSystemName = nameof(FileSystemName);
    public const string DirectoryName = nameof(DirectoryName);

     public AzureDatabricksConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
        providerName: "Azure Databricks Connector",
        componentName: "AzureDatabricksConnector",
        icon: "Resources.azuredatabricks.svg",
        domain: "https://azure.microsoft.com/en-au/products/databricks",
        about: "Supports publishing of data to Azure Databricks via its support for ADLSG2.",
        authMethods: GetAzureDataLakeAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to Azure Databricks.",
        guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
    {
    }

    protected override string CacheKeyword => "AzureDatabricksConnector";

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
