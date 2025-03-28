using CluedIn.Core.Connectors;

namespace CluedIn.Connector.DataLake.Common;

internal class DataLakeContainer : IConnectorContainer
{
    public string Name { get; set; }
    public string Id { get; set; }
    public string FullyQualifiedName { get; set; }
}
