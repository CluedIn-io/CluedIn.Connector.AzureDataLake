using System;

using Azure.Storage;
using Azure.Storage.Sas;

namespace CluedIn.Connector.AzureDataLake.Tests.Integration;

internal class SasTokenHelper
{
    public static string CreateSasToken(string accountName, string accountKey, bool hasStartTime = true)
    {
        if (string.IsNullOrWhiteSpace(accountName))
        {
            throw new ArgumentException("Account name cannot be null or whitespace", nameof(accountName));
        }

        if (string.IsNullOrWhiteSpace(accountKey))
        {
            throw new ArgumentException("Account key cannot be null or whitespace", nameof(accountKey));
        }
        var credential = new StorageSharedKeyCredential(accountName, accountKey);
        var sasBuilder = new AccountSasBuilder()
        {
            ResourceTypes = AccountSasResourceTypes.Container | AccountSasResourceTypes.Object,
            StartsOn = hasStartTime ? DateTimeOffset.UtcNow.AddMinutes(-5) : DateTimeOffset.MinValue,
            ExpiresOn = DateTimeOffset.UtcNow.AddHours(1),
            Services = AccountSasServices.Blobs,
            Protocol = SasProtocol.Https,
        };

        sasBuilder.SetPermissions(AccountSasPermissions.Read | AccountSasPermissions.Write | AccountSasPermissions.Delete | AccountSasPermissions.List | AccountSasPermissions.Create);
        string sasToken = sasBuilder.ToSasQueryParameters(credential).ToString();
        return sasToken;
    }
}
