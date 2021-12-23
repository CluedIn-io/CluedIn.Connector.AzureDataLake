using CluedIn.Core.Connectors;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeConnectorContainer : IConnectorContainer
    {
        public string Name { get; set; }
        public string Id { get; set; }
        public string FullyQualifiedName { get; set; }
    }
}
