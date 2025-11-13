using System;

using CluedIn.Core.Connectors;

namespace CluedIn.Connector.DataLake.Common;

internal static class ConnectorPropertyDataExtension
{
    internal static Type GetDataType(this ConnectorPropertyData connectorPropertyData)
    {
        var dataType = connectorPropertyData.DataType;
        if (dataType is EntityPropertyConnectorPropertyDataType entityPropertyType)
        {
            return entityPropertyType.Type;
        }

        return typeof(string);
    }
}
