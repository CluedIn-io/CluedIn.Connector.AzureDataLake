using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Providers;

using System;
using System.Collections.Generic;

namespace CluedIn.Connector.AmazonS3;

public class AmazonS3Constants : DataLakeConstants, IAmazonS3Constants
{
    internal static readonly Guid S3ProviderId = Guid.Parse("A3B5C7D9-1234-5678-9ABC-DEF012345678");

    public const string AccessKey = nameof(AccessKey);
    public const string SecretKey = nameof(SecretKey);
    public const string BucketName = nameof(BucketName);
    public const string Region = nameof(Region);
    public const string DirectoryName = nameof(DirectoryName);

    public AmazonS3Constants(ApplicationContext applicationContext) : base(S3ProviderId,
        providerName: "Amazon S3 Connector",
        componentName: "AmazonS3Connector",
        icon: "Resources.amazons3.svg",
        domain: "https://aws.amazon.com/s3/",
        about: "Supports publishing of data to Amazon S3.",
        authMethods: GetAmazonS3AuthMethods(applicationContext),
        guideDetails: "Supports publishing of data to Amazon S3.",
        guideInstructions: "Provide authentication instructions here, if applicable")
    {
    }

    protected override string CacheKeyword => "AmazonS3Connector";

    private static AuthMethods GetAmazonS3AuthMethods(ApplicationContext applicationContext)
    {
        var controls = new List<Control>
        {
            new()
            {
                Name = AccessKey,
                DisplayName = "Access Key",
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
            new()
            {
                Name = SecretKey,
                DisplayName = "Secret Key",
                Type = "password",
                IsRequired = true,
                ValidationRules = new List<Dictionary<string, string>>()
                {
                    new() {
                        { "regex", "\\s" },
                        { "message", "Spaces are not allowed" }
                    }
                },
            },
            new()
            {
                Name = BucketName,
                DisplayName = "Bucket Name",
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
            new()
            {
                Name = Region,
                DisplayName = "Region",
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
            new()
            {
                Name = DirectoryName,
                DisplayName = "Directory (Prefix)",
                Type = "input",
                IsRequired = false,
            },
        };

        controls.AddRange(GetAuthMethods(applicationContext, isArrayColumnOptionEnabled: true));

        return new AuthMethods
        {
            Token = controls
        };
    }
}
