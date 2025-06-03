using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.DataLake.Common.Tests.Integration;

public abstract partial class DataLakeConnectorTestsBase<TConnector, TJobDataFactory, TConstants>
    where TConnector : DataLakeConnector
    where TJobDataFactory : class, IDataLakeJobDataFactory
    where TConstants : class, IDataLakeConstants
{
    public string JsonExpectedOutput(StreamMode streamMode, VersionChangeType versionChangeType, bool isSingleObject = false)
    {
        var expectedJson = $$"""
        [
          {
            "Id": "f55c66dc-7881-55c9-889f-344992e71cb8",
            "Codes": [
              "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"
            ],
            "ContainerName": "test",
            "EntityType": "/Person",
            "Epoch": 1724192160000,
            "IncomingEdges": [
              {
                "FromReference": {
                  "Code": {
                    "Origin": {
                      "Code": "Acceptance",
                      "Id": null
                    },
                    "Value": "7c5591cf-861a-4642-861d-3b02485854a0",
                    "Key": "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0",
                    "Type": {
                      "IsEntityContainer": false,
                      "Root": null,
                      "Code": "/Person"
                    }
                  },
                  "Type": {
                    "IsEntityContainer": false,
                    "Root": null,
                    "Code": "/Person"
                  },
                  "Name": null,
                  "Properties": null,
                  "PropertyCount": null,
                  "EntityId": null,
                  "IsEmpty": false
                },
                "ToReference": {
                  "Code": {
                    "Origin": {
                      "Code": "Somewhere",
                      "Id": null
                    },
                    "Value": "1234",
                    "Key": "/EntityA#Somewhere:1234",
                    "Type": {
                      "IsEntityContainer": false,
                      "Root": null,
                      "Code": "/EntityA"
                    }
                  },
                  "Type": {
                    "IsEntityContainer": false,
                    "Root": null,
                    "Code": "/EntityA"
                  },
                  "Name": null,
                  "Properties": null,
                  "PropertyCount": null,
                  "EntityId": null,
                  "IsEmpty": false
                },
                "EdgeType": {
                  "Root": null,
                  "Code": "/EntityA"
                },
                "HasProperties": false,
                "Properties": {},
                "CreationOptions": 0,
                "Weight": null,
                "Version": 0
              }
            ],
            "Name": "Jean Luc Picard",
            "OriginEntityCode": "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0",
            "OutgoingEdges": [
              {
                "FromReference": {
                  "Code": {
                    "Origin": {
                      "Code": "Somewhere",
                      "Id": null
                    },
                    "Value": "5678",
                    "Key": "/EntityB#Somewhere:5678",
                    "Type": {
                      "IsEntityContainer": false,
                      "Root": null,
                      "Code": "/EntityB"
                    }
                  },
                  "Type": {
                    "IsEntityContainer": false,
                    "Root": null,
                    "Code": "/EntityB"
                  },
                  "Name": null,
                  "Properties": null,
                  "PropertyCount": null,
                  "EntityId": null,
                  "IsEmpty": false
                },
                "ToReference": {
                  "Code": {
                    "Origin": {
                      "Code": "Acceptance",
                      "Id": null
                    },
                    "Value": "7c5591cf-861a-4642-861d-3b02485854a0",
                    "Key": "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0",
                    "Type": {
                      "IsEntityContainer": false,
                      "Root": null,
                      "Code": "/Person"
                    }
                  },
                  "Type": {
                    "IsEntityContainer": false,
                    "Root": null,
                    "Code": "/Person"
                  },
                  "Name": null,
                  "Properties": null,
                  "PropertyCount": null,
                  "EntityId": null,
                  "IsEmpty": false
                },
                "EdgeType": {
                  "Root": null,
                  "Code": "/EntityB"
                },
                "HasProperties": false,
                "Properties": {},
                "CreationOptions": 0,
                "Weight": null,
                "Version": 0
              }
            ],
            "PersistHash": "etypzcezkiehwq8vw4oqog==",
            "PersistVersion": 1,
            "ProviderDefinitionId": "c444cda8-d9b5-45cc-a82d-fef28e08d55c",
            "Timestamp": "2024-08-21T03:16:00.0000000+05:00",
            "user.age": "123",
            "user.dobInDateTime": "2000-01-02T03:04:05",
            "user.dobInDateTimeOffset": "2000-01-02T03:04:05+12:34",
            "user.lastName": "Picard"
          }
        ]
        """;

        var jsonArray = JsonSerializer.Deserialize<JsonArray>(expectedJson);
        if (streamMode == StreamMode.EventStream)
        {
            var jsonObject = jsonArray[0].AsObject();
            jsonObject["ChangeType"] = versionChangeType.ToString();
        }

        return isSingleObject
            ? jsonArray[0].ToAlphabeticJsonString()
            : jsonArray.ToAlphabeticJsonString();
    }
}
