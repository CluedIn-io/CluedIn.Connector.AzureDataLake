using System;

namespace CluedIn.Connector.AzureDataLake.Extensions;

internal static class StringExtension
{
    internal static string GetFileExtension(this string outputFormat)
    {
        return outputFormat.ToUpper() switch
        {
            "JSON" => "json",
            "PARQUET" => "parquet",
            _ => throw new ArgumentNullException(nameof(outputFormat)),
        };
    }
}
