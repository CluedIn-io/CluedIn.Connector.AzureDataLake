using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Caching;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json;
using Parquet.Schema;
using Parquet;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using static Nest.MachineLearningUsage;
using ExecutionContext = CluedIn.Core.ExecutionContext;
using Parquet.Rows;
using Parquet.Serialization;
using CluedIn.Connector.AzureDataLake.Helpers;
using CluedIn.Core.Data.Relational;
using Apache.Arrow;
using Microsoft.VisualStudio.TestPlatform.PlatformAbstractions.Interfaces;
using System.Text;
using System.Net.Mail;
using System.Net;
using TZ4Net;
using System.Runtime.Serialization.Formatters.Binary;

namespace CluedIn.Connector.AzureDataLake.Tests.Integration
{

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

            Assert.Equal($@"[
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
]", content);

            await fsClient.GetDirectoryClient(Adl2_directoryName).DeleteAsync();
        }

        [Fact]
        public async void VerifyStoreData_1000EventStream()
        {
            InitializeMockObjects();
            InitializeStreamModel(StreamMode.EventStream);

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

            var stream = await fileClient.OpenReadAsync();
            var parquetTable = await ParquetReader.ReadTableFromStreamAsync(stream);
            Assert.NotNull(parquetTable);
            Assert.Equal(dummyDataCount, parquetTable.Count);

            //await DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName).DeleteAsync();
        }

        [Fact]
        public async void VerifyStoreData_1000Sync()
        {
            InitializeMockObjects();
            InitializeStreamModel(StreamMode.Sync);

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

            var stream = await fileClient.OpenReadAsync();
            var parquetTable = await ParquetReader.ReadTableFromStreamAsync(stream);
            Assert.NotNull(parquetTable);

            ///await DataLakeServiceClient.GetFileSystemClient(Adl2_fileSystemName).DeleteAsync();
        }

        [Fact]
        public async void VerifyStoreData_Sync()
        {
            var organizationId = Guid.NewGuid();
            var providerDefinitionId = Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c");

            var container = new WindsorContainer();

            container.Register(Component.For<ILogger<OrganizationDataStores>>()
                .Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

            container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
            container.Register(Component.For<IApplicationCache>().Instance(new Mock<IApplicationCache>().Object));

            var applicationContext = new ApplicationContext(container);
            var organization = new Organization(applicationContext, organizationId);

            var logger = new Mock<ILogger>().Object;

            var context = new ExecutionContext(applicationContext, organization, logger);

            var azureDataLakeConstantsMock = new Mock<IAzureDataLakeConstants>();
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);

            var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = $"xunit-{DateTime.Now.Ticks}";

            var connectorConnectionMock = new Mock<IConnectorConnectionV2>();
            connectorConnectionMock.Setup(x => x.Authentication).Returns(new Dictionary<string, object>()
            {
                { "AccountName", Adl2_accountName },
                { "AccountKey", Adl2_accountKey },
                { "FileSystemName", fileSystemName },
                { "DirectoryName", directoryName },
            });

            var connectorMock = new Mock<AzureDataLakeConnector>(
                new Mock<ILogger<AzureDataLakeConnector>>().Object,
                new AzureDataLakeClient(),
                azureDataLakeConstantsMock.Object
            );
            connectorMock.Setup(x => x.GetAuthenticationDetails(context, providerDefinitionId))
                .Returns(Task.FromResult(connectorConnectionMock.Object));
            connectorMock.CallBase = true;

            var connector = connectorMock.Object;

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

            var streamModel = new Mock<IReadOnlyStreamModel>();
            streamModel.Setup(x => x.ConnectorProviderDefinitionId).Returns(providerDefinitionId);
            streamModel.Setup(x => x.ContainerName).Returns("test");
            streamModel.Setup(x => x.Mode).Returns(StreamMode.Sync);

            await connector.StoreData(context, streamModel.Object, data);
            
            PathItem path;
            DataLakeFileSystemClient fsClient;

            var d = DateTime.Now;
            while (true)
            {
                if (DateTime.Now > d.AddSeconds(30))
                {
                    throw new TimeoutException();
                }

                if (DataLakeServiceClient.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }

                fsClient = DataLakeServiceClient.GetFileSystemClient(fileSystemName);

                var paths = fsClient.GetPaths(recursive: true)
                    .Where(p => p.IsDirectory == false)
                    .Where(p => p.Name.Contains(directoryName))
                    .ToArray();

                if (paths.Length == 0)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                path = paths.Single();

                if (path.ContentLength > 0)
                {
                    break;
                }
            }

            var fileClient = fsClient.GetFileClient(path.Name);

            var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();

            _testOutputHelper.WriteLine(content);

            Assert.Equal($@"{{
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
}}", content);

            data.ChangeType = VersionChangeType.Removed;
            await connector.StoreData(context, streamModel.Object, data);

            d = DateTime.Now;
            while (true)
            {
                if (DateTime.Now > d.AddSeconds(30))
                {
                    throw new TimeoutException("Timeout waiting for file to be deleted");
                }

                if (DataLakeServiceClient.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }

                fsClient = DataLakeServiceClient.GetFileSystemClient(fileSystemName);

                var paths = fsClient.GetPaths(recursive: true)
                    .Where(p => p.IsDirectory == false)
                    .Where(p => p.Name.Contains(directoryName))
                    .Select(p => p.Name)
                    .ToArray();

                if (paths.Contains(path.Name))
                {
                    Thread.Sleep(1000);
                    continue;
                }

                break;
            }

            await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
        }

        [Fact]
        public async void VerifyStoreData_Parquet_Sync()
        {
            var fileSystemName = "chadfilesystem" ?? $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = "Parquet-Test" ?? $"xunit-{DateTime.Now.Ticks}";
            var fileName = "testFile.parquet";

            var fsClient = DataLakeServiceClient.GetFileSystemClient(fileSystemName);
            var directoryClient = fsClient.GetDirectoryClient(directoryName);
            var dataLakeFileClient = directoryClient.GetFileClient(fileName);

            using var stream = new MemoryStream();

            var table = new Parquet.Rows.Table(new DataField<int>("id"), new DataField<string>("city"));

            //generate fake data
            for (int i = 1000; i < 1010; i++)
                table.Add(new Row(i, "record#" + i));

            using (ParquetWriter writer = await ParquetWriter.CreateAsync(table.Schema, stream))
                await writer.WriteAsync(table);

            var options = new DataLakeFileUploadOptions
            {
                HttpHeaders = new PathHttpHeaders { ContentType = "application/octet-stream" }
            };

            stream.Position = 0;
            var response = await dataLakeFileClient.UploadAsync(stream, options);

            await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
        }

        [Fact]
        public async void VerifyStoreData_Parquet2_Sync()
        {
            var fileSystemName = "chadfilesystem" ?? $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = "Parquet-Test" ?? $"xunit-{DateTime.Now.Ticks}";
            var fileName = "testFile2.parquet";

            var fsClient = DataLakeServiceClient.GetFileSystemClient(fileSystemName);
            var directoryClient = fsClient.GetDirectoryClient(directoryName);
            var dataLakeFileClient = directoryClient.GetFileClient(fileName);

            var schema = new ParquetSchema(new DataField<int>("id"), new DataField<string>("city"));
            var datas = new List<Dictionary<string, object>>();

            //generate fake data
            for (var i = 0; i < 1000; i++)
            {
                datas.Add(new Dictionary<string, object>
                {
                    { "id", i },
                    { "city", "record#" + i }
                });
            }

            using var stream = new MemoryStream();
            await ParquetSerializer.SerializeAsync(datas, stream);

            var options = new DataLakeFileUploadOptions
            {
                HttpHeaders = new PathHttpHeaders { ContentType = "application/octet-stream" }
            };

            stream.Position = 0;
            var response = await dataLakeFileClient.UploadAsync(stream, options);

            //await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
        }

        [Fact]
        public async void AzureDataLakeClient_SaveParquetDataTest()
        {
            var dataLakeClient = new AzureDataLakeClient();
            var containerName = "sample container";
            var authentication = new Dictionary<string, object>
            {
                { AzureDataLakeConstants.AccountName, Adl2_accountName },
                { AzureDataLakeConstants.AccountKey, Adl2_accountKey },
                { AzureDataLakeConstants.FileSystemName, "chadfilesystem" },
                { AzureDataLakeConstants.DirectoryName, "Parquet-Test" },
            };
            var configurations = new AzureDataLakeConnectorJobData(authentication, containerName);

            var properties = new List<ConnectorPropertyData>
            {
                new("FirstName", "Chad", new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("Name", VocabularyKeyVisibility.Visible))),
                new("LastName", "Naparite", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Text)),
                new("Display Name", "Richard Naparite", new EntityPropertyConnectorPropertyDataType(typeof(string))),
            };

            var data = properties.ToDictionary(x => x.Name, x => x.Value);
            data.Add("Id", Guid.NewGuid().ToString());
            data.Add("OriginEntityCode", "/Sample#OriginEntityCode:Value");
            data.Add("EntityType", "/AzureSample");
            data.Add("Codes", new[] { "Code1", "Code2", "Code3" });
            data.Add("ProviderDefinitionId", Guid.NewGuid().ToString());
            data.Add("ContainerName", containerName);
            //data.Add("OutgoingEdges",
            //    new List<EntityEdge>
            //    {
            //        new(
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue1")),
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue2")),
            //            EntityEdgeType.Owns),
            //        new(
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue3")),
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue4")),
            //            EntityEdgeType.Owns)
            //    });

            var parquetSchema = ParquetHelper.GenerateSchema(data, properties);
            var stream = await ParquetHelper.ToStream(parquetSchema, new List<Dictionary<string, object>> { data });

            await dataLakeClient.SaveData(configurations, stream, $"testFile3.parquet");
        }

        [Fact]
        public async Task SerializeModelTest()
        {
            var model = new SampleModel
            {
                Id = Guid.Parse("F6178E19-7168-449C-B4B6-F9810E86C1C2"),
                OriginEntityCode = "/Sample#OriginEntityCode:Value",
                EntityType = "/AzureSample",
                Codes = new[] { "Code1", "Code2", "Code3" },
                ProviderDefinitionId = Guid.NewGuid(),
                ContainerName = "sample container"
            };

            var schema = typeof(SampleModel).GetParquetSchema(true);

            // serialize to parquet
            var stream = new MemoryStream();
            _ = await ParquetSerializer.SerializeAsync(new List<SampleModel> { model }, stream);

            var fileSystemName = "chadfilesystem" ?? $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = "Parquet-Test" ?? $"xunit-{DateTime.Now.Ticks}";
            var fileName = "testFile4.parquet";

            var fsClient = DataLakeServiceClient.GetFileSystemClient(fileSystemName);
            var directoryClient = fsClient.GetDirectoryClient(directoryName);
            var dataLakeFileClient = directoryClient.GetFileClient(fileName);

            //var options = new DataLakeFileUploadOptions
            //{
            //    HttpHeaders = new PathHttpHeaders { ContentType = "application/octet-stream" }
            //};

            stream.Position = 0;
            var response = await dataLakeFileClient.UploadAsync(stream, overwrite: true);
        }

        [Fact]
        public async void ParquetTableTest()
        {
            var dataLakeClient = new AzureDataLakeClient();
            var containerName = "sample container";
            var authentication = new Dictionary<string, object>
            {
                { AzureDataLakeConstants.AccountName, Adl2_accountName },
                { AzureDataLakeConstants.AccountKey, Adl2_accountKey },
                { AzureDataLakeConstants.FileSystemName, "chadfilesystem" },
                { AzureDataLakeConstants.DirectoryName, "Parquet-Test" },
            };
            var configurations = new AzureDataLakeConnectorJobData(authentication, containerName);

            var properties = new List<ConnectorPropertyData>
            {
                new("FirstName", "Chad", new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("Name", VocabularyKeyVisibility.Visible))),
                new("LastName", "Naparite", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Text)),
                new("Display Name", "Richard Naparite", new EntityPropertyConnectorPropertyDataType(typeof(string))),
            };

            var data = properties.ToDictionary(x => x.Name, x => x.Value);
            data.Add("Id", Guid.NewGuid().ToString());
            data.Add("OriginEntityCode", "/Sample#OriginEntityCode:Value");
            data.Add("EntityType", "/AzureSample");
            data.Add("Codes", new[] { "Code1", "Code2", "Code3" });
            data.Add("ProviderDefinitionId", Guid.NewGuid().ToString());
            data.Add("ContainerName", containerName);
            //data.Add("OutgoingEdges",
            //    new List<EntityEdge>
            //    {
            //        new(
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue1")),
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue2")),
            //            EntityEdgeType.Owns),
            //        new(
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue3")),
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue4")),
            //            EntityEdgeType.Owns)
            //    });

            var parquetSchema = ParquetHelper.GenerateSchema(data, properties);

            var parquetTable = new Parquet.Rows.Table(parquetSchema)
            {
                new Row(data.Values.ToArray())
            };

            await dataLakeClient.SaveData(configurations, await ParquetHelper.ToStream(parquetTable), $"testFile5.parquet");
        }

        [Fact]
        public async void VocabsParquetTableTest()
        {
            var dataLakeClient = new AzureDataLakeClient();
            var containerName = "sample container";
            var authentication = new Dictionary<string, object>
            {
                { AzureDataLakeConstants.AccountName, Adl2_accountName },
                { AzureDataLakeConstants.AccountKey, Adl2_accountKey },
                { AzureDataLakeConstants.FileSystemName, "chadfilesystem" },
                { AzureDataLakeConstants.DirectoryName, "Parquet-Test" },
            };
            var configurations = new AzureDataLakeConnectorJobData(authentication, containerName);

            var properties = new List<ConnectorPropertyData>
            {
                new("FirstName", "Chad", new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("Name", VocabularyKeyVisibility.Visible))),
                new("LastName", "Naparite", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Text)),
                new("Display Name", "Richard Naparite", new EntityPropertyConnectorPropertyDataType(typeof(string))),

                new("BooleanField", true, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Boolean)),
                new("TextField", "Sample Text", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Text)),
                new("DateTimeField", DateTime.UtcNow, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.DateTime)),
                new("TimeField", DateTime.UtcNow, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Time)),
                new("DurationField", new TimeSpan(), new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Duration)),
                new("IntegerField", 129542, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Integer)),
                new("NumberField", 854663.25896f, new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Number)),
                new("UriField", "https://adlsg2testingapac.blob.core.windows.net/chadfilesystem/Parquet-Test/testFile5.parquet", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Uri)),
                new("GuidField", Guid.NewGuid().ToString(), new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Guid)),
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
                new("IdentifierField", Guid.NewGuid().ToString(), new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Identifier)),
                //new("Field", " Field", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Lookup)),
            };

            var data = properties.ToDictionary(x => x.Name, x => x.Value);
            //data.Add("OutgoingEdges",
            //    new List<EntityEdge>
            //    {
            //        new(
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue1")),
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue2")),
            //            EntityEdgeType.Owns),
            //        new(
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue3")),
            //            EntityReference.CreateByKnownCode(new EntityCode(Core.Data.EntityType.Organization, "Global", "SampleValue4")),
            //            EntityEdgeType.Owns)
            //    });

            var parquetSchema = ParquetHelper.GenerateSchema(data, properties);

            var parquetTable = new Parquet.Rows.Table(parquetSchema)
            {
                new Row(data.Values.ToArray())
            };

            await dataLakeClient.SaveData(configurations, await ParquetHelper.ToStream(parquetTable), $"testFile6.parquet");
        }
    }

    public class SampleModel
    {
        public Guid Id { get; set; }

        public string OriginEntityCode { get; set; }

        public string EntityType { get; set; }

        public string[] Codes { get; set; }

        public Guid ProviderDefinitionId { get; set; }

        public string ContainerName { get; set; }
    }
}
