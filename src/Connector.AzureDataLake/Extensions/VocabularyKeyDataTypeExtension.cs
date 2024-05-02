using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using System;
using System.Net.Mail;
using System.Net;
using TZ4Net;

namespace CluedIn.Connector.AzureDataLake.Extensions;

internal static class VocabularyKeyDataTypeExtension
{
    internal static Type ConvertToType(this VocabularyKeyDataType vocabKeyDataType)
    {
        return vocabKeyDataType switch
        {
            VocabularyKeyDataType.Boolean => typeof(bool),
            VocabularyKeyDataType.Text => typeof(string),
            VocabularyKeyDataType.DateTime => typeof(DateTime),
            VocabularyKeyDataType.Time => typeof(DateTime),
            VocabularyKeyDataType.Duration => typeof(TimeSpan),
            VocabularyKeyDataType.Integer => typeof(int),
            VocabularyKeyDataType.Number => typeof(float),
            VocabularyKeyDataType.Uri => typeof(string),
            VocabularyKeyDataType.Guid => typeof(Guid),
            VocabularyKeyDataType.Email => typeof(string),
            VocabularyKeyDataType.PhoneNumber => typeof(string),
            VocabularyKeyDataType.TimeZone => typeof(string),
            VocabularyKeyDataType.GeographyCity => typeof(string),
            VocabularyKeyDataType.GeographyState => typeof(string),
            VocabularyKeyDataType.GeographyCountry => typeof(string),
            VocabularyKeyDataType.GeographyCoordinates => typeof(string),
            VocabularyKeyDataType.GeographyLocation => typeof(string),
            VocabularyKeyDataType.Json => typeof(string),
            VocabularyKeyDataType.Xml => typeof(string),
            VocabularyKeyDataType.Html => typeof(string),
            VocabularyKeyDataType.IPAddress => typeof(string),
            VocabularyKeyDataType.Color => typeof(string),
            VocabularyKeyDataType.Money => typeof(decimal),
            VocabularyKeyDataType.Currency => typeof(string),
            VocabularyKeyDataType.PersonName => typeof(string),
            VocabularyKeyDataType.OrganizationName => typeof(string),
            VocabularyKeyDataType.Identifier => typeof(Guid),
            VocabularyKeyDataType.Lookup => typeof(string),
            _ => typeof(string),
        };
    }
}
