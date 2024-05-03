using System;

using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;

using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace CluedIn.Connector.AzureDataLake;

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
            VocabularyKeyDataType.Uri => typeof(Uri),
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
internal static class ConnectorPropertyDataExtension
{
    internal static Type GetDataType(this ConnectorPropertyData connectorPropertyData)
    {
        Type type = null;
        var dataType = connectorPropertyData.DataType;
        if (dataType is VocabularyKeyConnectorPropertyDataType vocabularyKeyType)
        {
            return vocabularyKeyType.VocabularyKey.DataType.ConvertToType();
        }
        else if (dataType is EntityPropertyConnectorPropertyDataType entityPropertyType)
        {
            return entityPropertyType.Type;
        }
        else if (dataType is VocabularyKeyDataTypeConnectorPropertyDataType vocabularyKeyDataTypeType)
        {
            return vocabularyKeyDataTypeType.VocabularyKeyDataType.ConvertToType();
        }

        throw new NotSupportedException($"{nameof(ConnectorPropertyDataType)} of type {dataType} is not supported.");
    }
}
