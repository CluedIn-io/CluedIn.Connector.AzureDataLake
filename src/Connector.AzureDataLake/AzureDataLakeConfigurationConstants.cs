using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;

using CluedIn.Integration.PrivateServices.Configuration;
// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.AzureDataLake;

public class AzureDataLakeConfigurationConstants : StorageConfigurationConstants, IAzureDataLakeConfigurationConstants
{
    internal static readonly Guid DataLakeProviderId = Guid.Parse("F6178E19-7168-449C-B4B6-F9810E86C1C2");

    public const string AccountName = nameof(AccountName);
    public const string FileSystemName = nameof(FileSystemName);
    public const string DirectoryName = nameof(DirectoryName);

    // Shared Key Authentication
    public const string AccountKey = nameof(AccountKey);

    // Service Principal Authentication
    public const string TenantId = nameof(TenantId);
    public const string ClientId = nameof(ClientId);
    public const string ClientSecret = nameof(ClientSecret);

    // Selectable Credential Method Authentication
    public const string AuthenticationMethod = nameof(AuthenticationMethod);

    // Key Vault Secret
    public const string UseKeyVault = nameof(UseKeyVault);
    public const string KeyVaultUri = nameof(KeyVaultUri);
    public const string KeyVaultSecretName = nameof(KeyVaultSecretName);

    public AzureDataLakeConfigurationConstants(ApplicationContext applicationContext) : base(DataLakeProviderId,
        providerName: "Azure DataLake Connector",
        componentName: "AzureDataLakeConnector",
        icon: "Resources.azuredatalake.svg",
        domain: "https://azure.microsoft.com/en-us/services/data-lake-analytics/",
        about: "Supports publishing of data to Azure Data Lake Storage Gen2.",
        authMethods: GetAzureDataLakeAuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to Azure DataLake.",
        guideInstructions: "Provide authentication instructions here, if applicable") // TODO: ROK:
    {
    }

    protected override string CacheKeyword => "AzureDataLakeConnector";

    private static AuthMethods GetAzureDataLakeAuthMethods(ApplicationContext applicationContext)
    {
        var nonSharedAccessKeyDependency = new ControlDisplayDependency
        {
            Name = AuthenticationMethod,
            Operator = ControlDependencyOperator.NotEquals,
            Value = AuthenticationMethods.SharedKey.ToString(),
            UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
        };
        var nonEmptyAuthenticationMethodDependency = new ControlDisplayDependency
        {
            Name = AuthenticationMethod,
            Operator = ControlDependencyOperator.Exists,
            UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
        };
        var useSharedAccessKeyDependency = new ControlDisplayDependency
        {
            Name = AuthenticationMethod,
            Operator = ControlDependencyOperator.Equals,
            Value = AuthenticationMethods.SharedKey.ToString(),
            UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
        };
        var servicePrincipalDependency = new ControlDisplayDependency
        {
            Name = AuthenticationMethod,
            Operator = ControlDependencyOperator.Equals,
            Value = AuthenticationMethods.ServicePrincipal.ToString(),
            UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
        };
        var workloadIdentityDependency = new ControlDisplayDependency
        {
            Name = AuthenticationMethod,
            Operator = ControlDependencyOperator.Equals,
            Value = AuthenticationMethods.WorkloadIdentity.ToString(),
            UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
        };
        var useKeyVaultDependency = new ControlDisplayDependency
        {
            Name = UseKeyVault,
            Operator = ControlDependencyOperator.Equals,
            Value = true.ToString().ToLowerInvariant(),
            UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
        };
        var controls = new List<Control>
        {
            new ()
            {
                Name = AccountName,
                DisplayName = "Account Name",
                Type = "input",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
            },
            new ()
            {
                Name = AuthenticationMethod,
                DisplayName = "Authentication Method",
                Type = "option",
                IsRequired = true,
                SourceType = ControlSourceType.Dynamic,
                Source = AzureDataLakeExtendedConfigurationProvider.AuthenticationSchemeSourceName,
                DisplayDependencies = [],
            },
            new ()
            {
                Name = AccountKey,
                DisplayName = "Account Key or Shared Access Signature Token",
                Type = "password",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
                DisplayDependencies = [useSharedAccessKeyDependency],
            },
            new ()
            {
                Name = TenantId,
                DisplayName = "Tenant Id",
                Type = "text",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
                DisplayDependencies = [nonSharedAccessKeyDependency, servicePrincipalDependency],
            },
            new ()
            {
                Name = ClientId,
                DisplayName = "Client Id",
                Type = "text",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
                DisplayDependencies = [nonSharedAccessKeyDependency, servicePrincipalDependency],
            },
            new ()
            {
                Name = ClientSecret,
                DisplayName = "Client Secret",
                Type = "password",
                IsRequired = true,
                ValidationRules = [],
                DisplayDependencies = [nonSharedAccessKeyDependency, servicePrincipalDependency],
            },
            new ()
            {
                Name = UseKeyVault,
                DisplayName = "Load Account Key or Shared Access Signature Token from Azure Key Vault Secret",
                Type = "checkbox",
                IsRequired = false,
                ValidationRules = [],
                DisplayDependencies = [nonSharedAccessKeyDependency, nonEmptyAuthenticationMethodDependency],
            },
            new ()
            {
                Name = KeyVaultUri,
                DisplayName = "Azure Key Vault URI",
                Type = "text",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
                DisplayDependencies = [nonSharedAccessKeyDependency, useKeyVaultDependency],
            },
            new ()
            {
                Name = KeyVaultSecretName,
                DisplayName = "Azure Key Vault Secret Name",
                Type = "text",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
                DisplayDependencies = [nonSharedAccessKeyDependency, useKeyVaultDependency],
            },
            new ()
            {
                Name = FileSystemName,
                DisplayName = "File System Name",
                Type = "input",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
            },
            new ()
            {
                Name = DirectoryName,
                DisplayName = "Directory Name",
                Type = "input",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
            },
        };

        controls.AddRange(GetAuthMethods(applicationContext, isArrayColumnOptionEnabled: true));
        controls.Add(
            new()
            {
                Name = ShouldEscapeVocabularyKeys,
                DisplayName = "Replace Non-Alphanumeric Characters in Column Names",
                Type = "checkbox",
                IsRequired = false,
                Help = """
                    Replaces characters in the column names that are not in this list ('a-z', 'A-Z', '0-9' and '_') with the character '_'.
                    Enable this if you plan to access the output file in Microsoft Purview.
                    """,
                DisplayDependencies = new[]
                {
                    new ControlDisplayDependency
                    {
                        Name = IsStreamCacheEnabled,
                        Operator = ControlDependencyOperator.Exists,
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                },
            }
        );
        controls.Add(
            new()
            {
                Name = ShouldWriteGuidAsString,
                DisplayName = "Write Guid as string",
                Type = "checkbox",
                IsRequired = false,
                Help = """
                    Write Guid values as string instead of byte array.
                    Enable this if you plan to access the output file in Microsoft Purview.
                    """,
                DisplayDependencies = new[]
                {
                    new ControlDisplayDependency
                    {
                        Name = OutputFormat,
                        Operator = ControlDependencyOperator.Equals,
                        Value = OutputFormats.Parquet.ToLowerInvariant(),
                        UnfulfilledAction = ControlDependencyUnfulfilledAction.Hidden,
                    },
                },
            }
        );

        return new AuthMethods
        {
            Token = controls
        };
    }

    public string WorkloadIdentityAuthenticationMethodEnabledKeyName => $"Streams.{CacheKeyword}.WorkloadIdentityAuthenticationMethodEnabled";

    public bool WorkloadIdentityAuthenticationMethodEnabledDefaultValue => false;
}
