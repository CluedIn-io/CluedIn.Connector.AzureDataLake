using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;

namespace CluedIn.Connector.Snowflake;

public class SnowflakeConfigurationConstants : StorageConfigurationConstants, ISnowflakeConfigurationConstants
{
    internal static readonly Guid SnowflakeProviderId = Guid.Parse("5211335C-D5C7-4D67-A76C-A27FEE51E893");

    public const string Account = nameof(Account);
    public const string User = nameof(User);
    public const string PrivateKey = nameof(PrivateKey);
    public const string PrivateKeyPassphrase = nameof(PrivateKeyPassphrase);
    public const string Database = nameof(Database);
    public const string Schema = nameof(Schema);
    public const string Warehouse = nameof(Warehouse);
    public const string Role = nameof(Role);
    public const string TableName = nameof(TableName);

    public SnowflakeConfigurationConstants(ApplicationContext applicationContext) : base(SnowflakeProviderId,
        providerName: "Snowflake Connector",
        componentName: "SnowflakeConnector",
        icon: "Resources.snowflake.svg",
        domain: "https://www.snowflake.com/",
        about: "Supports publishing of data to Snowflake using the Snowpipe Streaming REST API.",
        authMethods: GetSnowflakeAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to Snowflake using the Snowpipe Streaming REST API.",
        guideInstructions: "Provide a Snowflake account, key-pair authenticated user and target table to get started.")
    {
    }

    protected override string CacheKeyword => "SnowflakeConnector";

    private static AuthMethods GetSnowflakeAuthMethods(ApplicationContext applicationContext)
    {
        var controls = new List<Control>
        {
            new()
            {
                Name = Account,
                DisplayName = "Account Identifier",
                Type = "input",
                IsRequired = true,
                Help = "The Snowflake account identifier, e.g. 'cluedin-ci12345'.",
            },
            new()
            {
                Name = User,
                DisplayName = "User",
                Type = "input",
                IsRequired = true,
            },
            new()
            {
                Name = PrivateKey,
                DisplayName = "Private Key (PEM)",
                Type = "password",
                IsRequired = true,
                Help = "The PEM-encoded RSA private key configured for key-pair authentication on this user.",
            },
            new()
            {
                Name = PrivateKeyPassphrase,
                DisplayName = "Private Key Passphrase",
                Type = "password",
                IsRequired = false,
                Help = "Only required if the private key is encrypted.",
            },
            new()
            {
                Name = Database,
                DisplayName = "Database",
                Type = "input",
                IsRequired = true,
            },
            new()
            {
                Name = Schema,
                DisplayName = "Schema",
                Type = "input",
                IsRequired = true,
            },
            new()
            {
                Name = Warehouse,
                DisplayName = "Warehouse",
                Type = "input",
                IsRequired = true,
            },
            new()
            {
                Name = Role,
                DisplayName = "Role",
                Type = "input",
                IsRequired = false,
                Help = "When left blank, the user's default role is used.",
            },
            new()
            {
                Name = TableName,
                DisplayName = "Table Name",
                Type = "input",
                IsRequired = true,
                Help = "The target table in Snowflake that rows will be merged into.",
            },
        };

        controls.AddRange(
            GetAuthMethods(
                applicationContext,
                isCustomFileNamePatternSupported: false,
                isReducedFormats: true,
                isArrayColumnOptionEnabled: false,
                isForceStreamCache: true));

        return new AuthMethods
        {
            Token = controls
        };
    }
}
