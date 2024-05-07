using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Data;
using CluedIn.Core.Streams.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AutoFixture;

namespace CluedIn.Connector.AzureDataLake.Tests.Integration;

internal static class DataGenerator
{
    internal static ConnectorEntityData GenerateConnectorEntityData(
        VersionChangeType versionChangeType = VersionChangeType.Added,
        StreamMode streamMode = StreamMode.EventStream,
        IReadOnlyCollection<ConnectorPropertyData> properties = null)
    {
        var fixture = new Fixture();
        var originId = Guid.NewGuid();
        var firstName = fixture.Create<string>();
        var lastName = fixture.Create<string>();

        return new ConnectorEntityData(
            changeType: versionChangeType,
            streamMode: streamMode,
            entityId: Guid.NewGuid(),
            persistInfo: GetConnectorEntityPersistInfo(),
            previousPersistInfo: null,
            originEntityCode: EntityCode.FromKey($"/Person#Acceptance:{originId}"),
            entityType: "/Person",
            properties: properties ?? new[]
            {
                new ConnectorPropertyData("user.lastName", lastName,
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
                new ConnectorPropertyData("user.firstName", firstName,
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.firstName"))),
                new ConnectorPropertyData("Name", $"{firstName} {lastName}",
                    new EntityPropertyConnectorPropertyDataType(typeof(string))),
            },
            entityCodes: new IEntityCode[]
            {
                EntityCode.FromKey($"/Person#Acceptance:{originId}")
            },
            incomingEdges: new[]
            {
                new EntityEdge(
                    new EntityReference(EntityCode.FromKey($"/Person#Acceptance:{originId}")),
                    new EntityReference(EntityCode.FromKey("/EntityA#Somewhere:1234")),
                    "/EntityA")
            },
            outgoingEdges: new[]
            {
                new EntityEdge(new EntityReference(EntityCode.FromKey("/EntityB#Somewhere:5678")),
                    new EntityReference(EntityCode.FromKey($"/Person#Acceptance:{originId}")),
                    "/EntityB")
            });
    }

    internal static ConnectorEntityPersistInfo GetConnectorEntityPersistInfo()
    {
        var random = new Random();
        var randomByte = new byte[32];
        random.NextBytes(randomByte);

        return new ConnectorEntityPersistInfo(Convert.ToBase64String(randomByte).ToLowerInvariant(), 1);
    }
}
