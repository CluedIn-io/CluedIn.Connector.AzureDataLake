using CluedIn.Core.Connectors;
using System;

namespace CluedIn.Connector.AzureDataLake.Extensions;

internal static class ConnectorPropertyDataExtension
{
    internal static Type GetDataType(this ConnectorPropertyData connectorPropertyData)
    {
        Type type = null;
        if (connectorPropertyData.DataType.GetType() == typeof(VocabularyKeyConnectorPropertyDataType))
        {
            type = ((VocabularyKeyConnectorPropertyDataType)connectorPropertyData.DataType)
                .VocabularyKey.DataType.ConvertToType();
        }
        //else if (property.DataType.GetType() == typeof(SerializableVocabularyKeyConnectorPropertyDataType))
        //{

        //}
        else if (connectorPropertyData.DataType.GetType() == typeof(EntityPropertyConnectorPropertyDataType))
        {
            type = ((EntityPropertyConnectorPropertyDataType)connectorPropertyData.DataType).Type;
        }
        else if (connectorPropertyData.DataType.GetType() == typeof(VocabularyKeyDataTypeConnectorPropertyDataType))
        {
            type = ((VocabularyKeyDataTypeConnectorPropertyDataType)connectorPropertyData.DataType)
                .VocabularyKeyDataType.ConvertToType();
        }

        return type;
    }
}
