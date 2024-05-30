using System;

using CluedIn.Core.Connectors;

namespace CluedIn.Connector.DataLake.Common;

internal static class ConnectorPropertyDataExtension
{
    internal static Type GetDataType(this ConnectorPropertyData connectorPropertyData)
    {
        var dataType = connectorPropertyData.DataType;
        if (dataType is VocabularyKeyConnectorPropertyDataType vocabularyKeyType)
        {
            return typeof(string);
        }
        else if (dataType is EntityPropertyConnectorPropertyDataType entityPropertyType)
        {
            return entityPropertyType.Type;
        }
        else if (dataType is VocabularyKeyDataTypeConnectorPropertyDataType vocabularyKeyDataTypeType)
        {
            return typeof(string);
        }

        throw new NotSupportedException($"{nameof(ConnectorPropertyDataType)} of type {dataType} is not supported.");
    }
}
