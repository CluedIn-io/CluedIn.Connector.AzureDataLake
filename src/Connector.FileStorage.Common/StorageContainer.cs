using CluedIn.Core.Connectors;

namespace CluedIn.Connector.FileStorage.Common;

public class StorageContainer : IConnectorContainer
{
    public string Name { get; set; }
    public string Id { get; set; }
    public string FullyQualifiedName { get; set; }
}
