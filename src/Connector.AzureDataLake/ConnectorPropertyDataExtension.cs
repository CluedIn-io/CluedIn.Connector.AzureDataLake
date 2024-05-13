using System;

using CluedIn.Core.Connectors;

namespace CluedIn.Connector.AzureDataLake;

internal static class ConnectorPropertyDataExtension
{
    internal static Type GetDataType(this ConnectorPropertyData connectorPropertyData)
    {
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
