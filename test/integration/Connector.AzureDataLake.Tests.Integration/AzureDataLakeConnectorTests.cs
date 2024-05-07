using Azure.Storage.Files.DataLake;
using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
using FluentAssertions.Json;
using Parquet;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Newtonsoft.Json;
using System.Globalization;
using Newtonsoft.Json.Linq;

namespace CluedIn.Connector.AzureDataLake.Tests.Integration;

public class AzureDataLakeConnectorTests : ConnectorBaseTest
{
    private readonly ITestOutputHelper _testOutputHelper;

    public AzureDataLakeConnectorTests(ITestOutputHelper testOutputHelper) : base()
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async void VerifyStoreData_EventStream()
    {
        InitializeMockObjects();
        InitializeStreamModel(StreamMode.EventStream, "JSON");

        var data = new ConnectorEntityData(VersionChangeType.Added, StreamMode.EventStream,
            Guid.Parse("f55c66dc-7881-55c9-889f-344992e71cb8"),
            new ConnectorEntityPersistInfo("etypzcezkiehwq8vw4oqog==", 1), null,
            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"),
            "/Person",
            new[]
            {
                new ConnectorPropertyData("user.lastName", "Picard",
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
                new ConnectorPropertyData("Name", "Jean Luc Picard",
                    new EntityPropertyConnectorPropertyDataType(typeof(string))),
            },
            new IEntityCode[] { EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0") },
            new[]
            {
                new EntityEdge(
                    new EntityReference(
                        EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                    new EntityReference(EntityCode.FromKey("/EntityA#Somewhere:1234")), "/EntityA")
            },
            new[]
            {
                new EntityEdge(new EntityReference(EntityCode.FromKey("/EntityB#Somewhere:5678")),
                    new EntityReference(
                        EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                    "/EntityB")
            });

        await AzureDataLakeConnector.StoreData(Context, StreamModel, data);
        
        var path = GetPathItems().Single();

        var fsClient = DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName);
        var fileClient = fsClient.GetFileClient(path.Name);

        var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();

        _testOutputHelper.WriteLine(content);
        var actualJson = JsonConvert.DeserializeObject<JToken>(content);
        var expectedJson = JsonConvert.DeserializeObject<JToken>($@"[
  {{
    ""user.lastName"": ""Picard"",
    ""Name"": ""Jean Luc Picard"",
    ""Id"": ""f55c66dc-7881-55c9-889f-344992e71cb8"",
    ""PersistHash"": ""etypzcezkiehwq8vw4oqog=="",
    ""OriginEntityCode"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
    ""EntityType"": ""/Person"",
    ""Codes"": [
      ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0""
    ],
    ""ProviderDefinitionId"": ""c444cda8-d9b5-45cc-a82d-fef28e08d55c"",
    ""ContainerName"": ""test"",
    ""OutgoingEdges"": [
      {{
        ""FromReference"": {{
          ""Code"": {{
            ""Origin"": {{
              ""Code"": ""Somewhere"",
              ""Id"": null
            }},
            ""Value"": ""5678"",
            ""Key"": ""/EntityB#Somewhere:5678"",
            ""Type"": {{
              ""IsEntityContainer"": false,
              ""Root"": null,
              ""Code"": ""/EntityB""
            }}
          }},
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/EntityB""
          }},
          ""Name"": null,
          ""Properties"": null,
          ""PropertyCount"": null,
          ""EntityId"": null,
          ""IsEmpty"": false
        }},
        ""ToReference"": {{
          ""Code"": {{
            ""Origin"": {{
              ""Code"": ""Acceptance"",
              ""Id"": null
            }},
            ""Value"": ""7c5591cf-861a-4642-861d-3b02485854a0"",
            ""Key"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
            ""Type"": {{
              ""IsEntityContainer"": false,
              ""Root"": null,
              ""Code"": ""/Person""
            }}
          }},
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/Person""
          }},
          ""Name"": null,
          ""Properties"": null,
          ""PropertyCount"": null,
          ""EntityId"": null,
          ""IsEmpty"": false
        }},
        ""EdgeType"": {{
          ""Root"": null,
          ""Code"": ""/EntityB""
        }},
        ""HasProperties"": false,
        ""Properties"": {{}},
        ""CreationOptions"": 0,
        ""Weight"": null,
        ""Version"": 0
      }}
    ],
    ""IncomingEdges"": [
      {{
        ""FromReference"": {{
          ""Code"": {{
            ""Origin"": {{
              ""Code"": ""Acceptance"",
              ""Id"": null
            }},
            ""Value"": ""7c5591cf-861a-4642-861d-3b02485854a0"",
            ""Key"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
            ""Type"": {{
              ""IsEntityContainer"": false,
              ""Root"": null,
              ""Code"": ""/Person""
            }}
          }},
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/Person""
          }},
          ""Name"": null,
          ""Properties"": null,
          ""PropertyCount"": null,
          ""EntityId"": null,
          ""IsEmpty"": false
        }},
        ""ToReference"": {{
          ""Code"": {{
            ""Origin"": {{
              ""Code"": ""Somewhere"",
              ""Id"": null
            }},
            ""Value"": ""1234"",
            ""Key"": ""/EntityA#Somewhere:1234"",
            ""Type"": {{
              ""IsEntityContainer"": false,
              ""Root"": null,
              ""Code"": ""/EntityA""
            }}
          }},
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/EntityA""
          }},
          ""Name"": null,
          ""Properties"": null,
          ""PropertyCount"": null,
          ""EntityId"": null,
          ""IsEmpty"": false
        }},
        ""EdgeType"": {{
          ""Root"": null,
          ""Code"": ""/EntityA""
        }},
        ""HasProperties"": false,
        ""Properties"": {{}},
        ""CreationOptions"": 0,
        ""Weight"": null,
        ""Version"": 0
      }}
    ],
    ""ChangeType"": ""Added""
  }}
]");
        actualJson.Should().BeEquivalentTo(expectedJson);

        await fsClient.GetDirectoryClient(Adl2_directoryName).DeleteAsync();
    }


    [Fact]
    public async void VerifyStoreData_Sync()
    {
        InitializeMockObjects();
        InitializeStreamModel(StreamMode.Sync, "JSON");

        var data = new ConnectorEntityData(VersionChangeType.Added, StreamMode.Sync,
            Guid.Parse("f55c66dc-7881-55c9-889f-344992e71cb8"),
            new ConnectorEntityPersistInfo("etypzcezkiehwq8vw4oqog==", 1), null,
            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"),
            "/Person",
            new[]
            {
                new ConnectorPropertyData("user.lastName", "Picard",
                    new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
                new ConnectorPropertyData("Name", "Jean Luc Picard",
                    new EntityPropertyConnectorPropertyDataType(typeof(string))),
            },
            new IEntityCode[] { EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0") },
            new[]
            {
                new EntityEdge(
                    new EntityReference(
                        EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                    new EntityReference(EntityCode.FromKey("/EntityA#Somewhere:1234")), "/EntityA")
            },
            new[]
            {
                new EntityEdge(new EntityReference(EntityCode.FromKey("/EntityB#Somewhere:5678")),
                    new EntityReference(
                        EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                    "/EntityB")
            });

        await AzureDataLakeConnector.StoreData(Context, StreamModel, data);

        var path = GetPathItems().Single();

        var fsClient = DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName);
        var fileClient = fsClient.GetFileClient(path.Name);

        var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();

        _testOutputHelper.WriteLine(content);

        var actualJson = JsonConvert.DeserializeObject<JToken>(content);
        var expectedJson = JsonConvert.DeserializeObject<JToken>($@"{{
  ""user.lastName"": ""Picard"",
  ""Name"": ""Jean Luc Picard"",
  ""Id"": ""f55c66dc-7881-55c9-889f-344992e71cb8"",
  ""PersistHash"": ""etypzcezkiehwq8vw4oqog=="",
  ""OriginEntityCode"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
  ""EntityType"": ""/Person"",
  ""Codes"": [
    ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0""
  ],
  ""ProviderDefinitionId"": ""c444cda8-d9b5-45cc-a82d-fef28e08d55c"",
  ""ContainerName"": ""test"",
  ""OutgoingEdges"": [
    {{
      ""FromReference"": {{
        ""Code"": {{
          ""Origin"": {{
            ""Code"": ""Somewhere"",
            ""Id"": null
          }},
          ""Value"": ""5678"",
          ""Key"": ""/EntityB#Somewhere:5678"",
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/EntityB""
          }}
        }},
        ""Type"": {{
          ""IsEntityContainer"": false,
          ""Root"": null,
          ""Code"": ""/EntityB""
        }},
        ""Name"": null,
        ""Properties"": null,
        ""PropertyCount"": null,
        ""EntityId"": null,
        ""IsEmpty"": false
      }},
      ""ToReference"": {{
        ""Code"": {{
          ""Origin"": {{
            ""Code"": ""Acceptance"",
            ""Id"": null
          }},
          ""Value"": ""7c5591cf-861a-4642-861d-3b02485854a0"",
          ""Key"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/Person""
          }}
        }},
        ""Type"": {{
          ""IsEntityContainer"": false,
          ""Root"": null,
          ""Code"": ""/Person""
        }},
        ""Name"": null,
        ""Properties"": null,
        ""PropertyCount"": null,
        ""EntityId"": null,
        ""IsEmpty"": false
      }},
      ""EdgeType"": {{
        ""Root"": null,
        ""Code"": ""/EntityB""
      }},
      ""HasProperties"": false,
      ""Properties"": {{}},
      ""CreationOptions"": 0,
      ""Weight"": null,
      ""Version"": 0
    }}
  ],
  ""IncomingEdges"": [
    {{
      ""FromReference"": {{
        ""Code"": {{
          ""Origin"": {{
            ""Code"": ""Acceptance"",
            ""Id"": null
          }},
          ""Value"": ""7c5591cf-861a-4642-861d-3b02485854a0"",
          ""Key"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/Person""
          }}
        }},
        ""Type"": {{
          ""IsEntityContainer"": false,
          ""Root"": null,
          ""Code"": ""/Person""
        }},
        ""Name"": null,
        ""Properties"": null,
        ""PropertyCount"": null,
        ""EntityId"": null,
        ""IsEmpty"": false
      }},
      ""ToReference"": {{
        ""Code"": {{
          ""Origin"": {{
            ""Code"": ""Somewhere"",
            ""Id"": null
          }},
          ""Value"": ""1234"",
          ""Key"": ""/EntityA#Somewhere:1234"",
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/EntityA""
          }}
        }},
        ""Type"": {{
          ""IsEntityContainer"": false,
          ""Root"": null,
          ""Code"": ""/EntityA""
        }},
        ""Name"": null,
        ""Properties"": null,
        ""PropertyCount"": null,
        ""EntityId"": null,
        ""IsEmpty"": false
      }},
      ""EdgeType"": {{
        ""Root"": null,
        ""Code"": ""/EntityA""
      }},
      ""HasProperties"": false,
      ""Properties"": {{}},
      ""CreationOptions"": 0,
      ""Weight"": null,
      ""Version"": 0
    }}
  ]
}}");

        actualJson.Should().BeEquivalentTo(expectedJson);

        data.ChangeType = VersionChangeType.Removed;
        await AzureDataLakeConnector.StoreData(Context, StreamModel, data);

        var isFileDeleted = IsFileDeleted(path);

        Assert.True(isFileDeleted);

        await fsClient.GetDirectoryClient(Adl2_directoryName).DeleteAsync();
    }

    [Theory]
    [InlineData("PARQUET")]
    [InlineData("JSON")]
    public async void VerifyStoreData_1000EventStream(string outputFormat)
    {
        InitializeMockObjects();
        InitializeStreamModel(StreamMode.EventStream, outputFormat);

        var dummyDataCount = 1000;

        var tasks = Enumerable.Range(0, dummyDataCount)
            .Select(i =>
                Task.Run(async () =>
                {
                    await AzureDataLakeConnector.StoreData(Context, StreamModel, DataGenerator.GenerateConnectorEntityData(streamMode: StreamModel.Mode.Value));
                }))
            .ToArray();

        Task.WaitAll(tasks);

        Assert.False(tasks.All(t => t.IsFaulted));

        var path = GetPathItems().Single();

        var fsClient = DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName);
        var fileClient = fsClient.GetFileClient(path.Name);

        if (outputFormat == "PARQUET")
        {
            var stream = await fileClient.OpenReadAsync();
            var parquetTable = await ParquetReader.ReadTableFromStreamAsync(stream);
            Assert.NotNull(parquetTable);
            Assert.Equal(dummyDataCount, parquetTable.Count);
        }
        else if (outputFormat == "JSON")
        {
            var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();
            var jsonData = JsonConvert.DeserializeObject<JObject[]>(content);
            Assert.NotNull(jsonData);
            Assert.Equal(dummyDataCount, jsonData.Length);
        }
        await DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName).DeleteAsync();
    }

    [Theory]
    [InlineData("PARQUET")]
    [InlineData("JSON")]
    public async void VerifyStoreData_1000Sync(string outputFormat)
    {
        InitializeMockObjects();
        InitializeStreamModel(StreamMode.Sync, outputFormat);

        var dummyDataCount = 1000;

        var tasks = Enumerable.Range(0, dummyDataCount)
            .Select(i =>
                Task.Run(async () =>
                {
                    await AzureDataLakeConnector.StoreData(Context, StreamModel, DataGenerator.GenerateConnectorEntityData(streamMode: StreamModel.Mode.Value));
                }))
            .ToArray();

        Task.WaitAll(tasks);

        Assert.False(tasks.All(t => t.IsFaulted));

        var paths = GetPathItems();
        Assert.Equal(dummyDataCount, paths.Length);

        var fsClient = DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName);
        var fileClient = fsClient.GetFileClient(paths[0].Name);

        if (outputFormat == "PARQUET")
        {
            var stream = await fileClient.OpenReadAsync();
            var parquetTable = await ParquetReader.ReadTableFromStreamAsync(stream);
            Assert.NotNull(parquetTable);
        }
        else if (outputFormat == "JSON")
        {
            var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();
            var jsonData = JsonConvert.DeserializeObject<JObject>(content);
            Assert.NotNull(jsonData);
        }

        await DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName).DeleteAsync();
    }

    [Theory]
    [InlineData(StreamMode.EventStream, "PARQUET")]
    [InlineData(StreamMode.EventStream, "JSON")]
    [InlineData(StreamMode.Sync, "PARQUET")]
    [InlineData(StreamMode.Sync, "JSON")]
    public async Task CompleteConnectorPropertyDataTest(StreamMode streamMode, string outputFormat)
    {
        InitializeMockObjects();
        InitializeStreamModel(streamMode, outputFormat);

        var properties = new List<ConnectorPropertyData>
        {
            new("FirstName", "Chad", new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("Name", VocabularyKeyVisibility.Visible))),
            new("LastName", "Naparite", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Text)),

            new("DisplayName", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),//nullable
            new("Aliases", Array.Empty<string>(), new EntityPropertyConnectorPropertyDataType(typeof(string[]))),
            new("Uris", Array.Empty<IEntityUri>(), new EntityPropertyConnectorPropertyDataType(typeof(IEntityUri[]))),
            new("Authors", Array.Empty<PersonReference>(), new EntityPropertyConnectorPropertyDataType(typeof(PersonReference[]))),
            new("Sentiment", null, new EntityPropertyConnectorPropertyDataType(typeof(double?))),
            new("CreatedDate", null, new EntityPropertyConnectorPropertyDataType(typeof(DateTimeOffset?))),
            new("DocumentSize", null, new EntityPropertyConnectorPropertyDataType(typeof(long?))),
            new("Description", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            new("Type", (EntityType)"/Employee", new EntityPropertyConnectorPropertyDataType(typeof(EntityType))),
            new("SystemTagName", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            new("Culture", null, new EntityPropertyConnectorPropertyDataType(typeof(CultureInfo))),
            new("Name", "Richard Naparite", new EntityPropertyConnectorPropertyDataType(typeof(CultureInfo))),
            new("ModifiedDate", null, new EntityPropertyConnectorPropertyDataType(typeof(DateTimeOffset?))),
            new("IsExternalData", null, new EntityPropertyConnectorPropertyDataType(typeof(bool?))),
            new("ChangeVerb", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            new("Uri", new Uri("https://google.com/search/name?query=value"), new EntityPropertyConnectorPropertyDataType(typeof(Uri))),
            new("SortDate", new DateTimeOffset(DateTime.Today), new EntityPropertyConnectorPropertyDataType(typeof(DateTimeOffset?))),
            new("ContainsUserInput", false, new EntityPropertyConnectorPropertyDataType(typeof(bool))),
            new("IndexedText", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            new("DocumentFileName", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            new("ParentRevision", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            new("Revision", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            new("LastChangedBy", null, new EntityPropertyConnectorPropertyDataType(typeof(PersonReference))),
            new("IsSensitiveInformation", null, new EntityPropertyConnectorPropertyDataType(typeof(bool?))),
            new("DocumentFileHashCode", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            new("DiscoveryDate", new DateTimeOffset(DateTime.Today), new EntityPropertyConnectorPropertyDataType(typeof(DateTimeOffset?))),
            new("Encoding", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),
            new("IsDeleted", null, new EntityPropertyConnectorPropertyDataType(typeof(bool?))),
            new("IsShadowEntity", false, new EntityPropertyConnectorPropertyDataType(typeof(bool))),
            new("LastProcessedDate", new DateTimeOffset(DateTime.Today), new EntityPropertyConnectorPropertyDataType(typeof(DateTimeOffset?))),
            new("Tags", Array.Empty<ITag>(), new EntityPropertyConnectorPropertyDataType(typeof(ITag[]))),
            new("TimeToLive", (long)0, new EntityPropertyConnectorPropertyDataType(typeof(long))),
            new("DocumentMimeType", null, new EntityPropertyConnectorPropertyDataType(typeof(string))),

            new("BooleanField", true, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Boolean)),
            new("TextField", "Sample Text", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Text)),
            new("DateTimeField", DateTime.UtcNow, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.DateTime)),
            new("TimeField", DateTime.UtcNow, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Time)),
            new("DurationField", new TimeSpan(), new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Duration)),
            new("IntegerField", 129542, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Integer)),
            new("NumberField", 854663.25896f, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Number)),
            new("UriField", new Uri("https://adlsg2testingapac.blob.core.windows.net/chadfilesystem/Parquet-Test/testFile5.parquet"), new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Uri)),
            new("GuidField", Guid.NewGuid(), new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Guid)),
            new("EmailField", "rna@cluedin.com", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Email)),
            new("PhoneNumberField", "+639954566870", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.PhoneNumber)),
            new("TimeZoneField", "Sample Timezone", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.TimeZone)),
            new("GeographyCityField", "Sample GeographyCity", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.GeographyCity)),
            new("GeographyStateField", "Sample GeographyState", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.GeographyState)),
            new("GeographyCountryField", "Sample GeographyCountry", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.GeographyCountry)),
            new("GeographyCoordinatesField", "Sample GeographyCoordinates", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.GeographyCoordinates)),
            new("GeographyLocationField", "Sample GeographyLocation", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.GeographyLocation)),
            new("JsonField", "{}", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Json)),
            new("XmlField", "<Sample></Sample>", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Xml)),
            new("HtmlField", "<html></html>", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Html)),
            new("IPAddressField", "192.168.0.1", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.IPAddress)),
            new("ColorField", "Blue", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Color)),
            new("MoneyField", 4528.26M, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Money)),
            new("CurrencyField", "Peso", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Currency)),
            new("PersonNameField", "Richard Naparite", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.PersonName)),
            new("OrganizationNameField", "foobar", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.OrganizationName)),
            new("IdentifierField", Guid.NewGuid(), new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Identifier)),
            //new("Field", " Field", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Lookup)),
        };

        await AzureDataLakeConnector.StoreData(Context, StreamModel, DataGenerator.GenerateConnectorEntityData(properties: properties, streamMode: StreamModel.Mode.Value));

        var path = GetPathItems().Single();

        var fsClient = DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName);
        var fileClient = fsClient.GetFileClient(path.Name);

        if (outputFormat == "PARQUET")
        {
            var stream = await fileClient.OpenReadAsync();
            var parquetTable = await ParquetReader.ReadTableFromStreamAsync(stream);
            Assert.NotNull(parquetTable);
        }
        else if (outputFormat == "JSON")
        {
            var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();
            var jsonData = JsonConvert.DeserializeObject<JToken>(content);
            Assert.NotNull(jsonData);
        }

        await DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName).DeleteAsync();
    }
}
