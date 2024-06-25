using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Caching;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Moq;

using ParquetSharp;

using Xunit;
using Xunit.Abstractions;

using ExecutionContext = CluedIn.Core.ExecutionContext;
using Encoding = System.Text.Encoding;
using Newtonsoft.Json.Linq;

namespace CluedIn.Connector.AzureDataLake.Tests.Integration
{

    public class AzureDataLakeConnectorTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public AzureDataLakeConnectorTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async void VerifyStoreData_EventStream()
        {
            var organizationId = Guid.NewGuid();
            var providerDefinitionId = Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c");

            var container = new WindsorContainer();

            container.Register(Component.For<ILogger<OrganizationDataStores>>()
                .Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

            container.Register(Component.For<ISystemConnectionStrings>().Instance(new Mock<ISystemConnectionStrings>().Object));
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

            var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
            Assert.NotNull(accountName);
            var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
            Assert.NotNull(accountKey);

            var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = $"xunit-{DateTime.Now.Ticks}";

            var connectorConnectionMock = new Mock<IConnectorConnectionV2>();
            connectorConnectionMock.Setup(x => x.Authentication).Returns(new Dictionary<string, object>()
            {
                { "AccountName", accountName },
                { "AccountKey", accountKey },
                { "FileSystemName", fileSystemName },
                { "DirectoryName", directoryName },
            });

            var jobDataFactory = new Mock<AzureDataLakeJobDataFactory>();
            var connectorMock = new Mock<AzureDataLakeConnector>(
                new Mock<ILogger<AzureDataLakeConnector>>().Object,
                new AzureDataLakeClient(),
                azureDataLakeConstantsMock.Object,
                jobDataFactory.Object
            );

            jobDataFactory.Setup(x => x.GetConfiguration(context, providerDefinitionId, It.IsAny<string>()  ))
                .ReturnsAsync(new AzureDataLakeConnectorJobData(connectorConnectionMock.Object.Authentication.ToDictionary(x => x.Key, x => x.Value)));
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
                    new ConnectorPropertyData("user.age", "123",
                        new VocabularyKeyConnectorPropertyDataType(
                            new VocabularyKey("user.age", dataType: VocabularyKeyDataType.Integer)
                            {
                                Storage = VocabularyKeyStorage.Typed,
                            })),
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
            streamModel.Setup(x => x.Mode).Returns(StreamMode.EventStream);
            streamModel.Setup(x => x.ExportIncomingEdges).Returns(true);
            streamModel.Setup(x => x.ExportOutgoingEdges).Returns(true);

            await connector.StoreData(context, streamModel.Object, data);
            var client = new DataLakeServiceClient(new Uri($"https://{accountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(accountName, accountKey));

            try
            {
                var path = await WaitForFileToBeCreated(fileSystemName, directoryName, client);
                var fsClient = client.GetFileSystemClient(fileSystemName);
                var fileClient = fsClient.GetFileClient(path.Name);

                var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();

                _testOutputHelper.WriteLine(content);

                Assert.Equal(
                    $$"""
                    [
                      {
                        "user.lastName": "Picard",
                        "user.age": "123",
                        "Name": "Jean Luc Picard",
                        "Id": "f55c66dc-7881-55c9-889f-344992e71cb8",
                        "PersistHash": "etypzcezkiehwq8vw4oqog==",
                        "PersistVersion": 1,
                        "OriginEntityCode": "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0",
                        "EntityType": "/Person",
                        "Codes": [
                          "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"
                        ],
                        "ProviderDefinitionId": "c444cda8-d9b5-45cc-a82d-fef28e08d55c",
                        "ContainerName": "test",
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
                        "ChangeType": "Added"
                      }
                    ]
                    """, content);

                await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
            }
            finally
            {
                await DeleteFileSystem(client, fileSystemName);
            }
        }

        [Fact]
        public async void VerifyStoreData_Sync_WithoutStreamCache()
        {
            var organizationId = Guid.NewGuid();
            var providerDefinitionId = Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c");

            var container = new WindsorContainer();

            container.Register(Component.For<ILogger<OrganizationDataStores>>()
                .Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

            container.Register(Component.For<ISystemConnectionStrings>().Instance(new Mock<ISystemConnectionStrings>().Object));
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

            var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
            Assert.NotNull(accountName);
            var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
            Assert.NotNull(accountKey);

            var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = $"xunit-{DateTime.Now.Ticks}";

            var connectorConnectionMock = new Mock<IConnectorConnectionV2>();
            connectorConnectionMock.Setup(x => x.Authentication).Returns(new Dictionary<string, object>()
            {
                { "AccountName", accountName },
                { "AccountKey", accountKey },
                { "FileSystemName", fileSystemName },
                { "DirectoryName", directoryName },
            });

            var jobDataFactory = new Mock<AzureDataLakeJobDataFactory>();
            var connectorMock = new Mock<AzureDataLakeConnector>(
                new Mock<ILogger<AzureDataLakeConnector>>().Object,
                new AzureDataLakeClient(),
                azureDataLakeConstantsMock.Object,
                jobDataFactory.Object
            );

            jobDataFactory.Setup(x => x.GetConfiguration(context, providerDefinitionId, It.IsAny<string>()))
                .ReturnsAsync(new AzureDataLakeConnectorJobData(connectorConnectionMock.Object.Authentication.ToDictionary(x => x.Key, x => x.Value)));
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
                    new ConnectorPropertyData("user.age", "123",
                        new VocabularyKeyConnectorPropertyDataType(
                            new VocabularyKey("user.age", dataType: VocabularyKeyDataType.Integer)
                            {
                                Storage = VocabularyKeyStorage.Typed,
                            })),
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
            streamModel.Setup(x => x.ExportIncomingEdges).Returns(true);
            streamModel.Setup(x => x.ExportOutgoingEdges).Returns(true);

            await connector.StoreData(context, streamModel.Object, data);
            var client = new DataLakeServiceClient(new Uri($"https://{accountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(accountName, accountKey));

            try
            {
                var path = await WaitForFileToBeCreated(fileSystemName, directoryName, client);
                var fsClient = client.GetFileSystemClient(fileSystemName);
                var fileClient = fsClient.GetFileClient(path.Name);

                var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();

                _testOutputHelper.WriteLine(content);

                Assert.Equal(
                    $$"""
                    {
                      "user.lastName": "Picard",
                      "user.age": "123",
                      "Name": "Jean Luc Picard",
                      "Id": "f55c66dc-7881-55c9-889f-344992e71cb8",
                      "PersistHash": "etypzcezkiehwq8vw4oqog==",
                      "PersistVersion": 1,
                      "OriginEntityCode": "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0",
                      "EntityType": "/Person",
                      "Codes": [
                        "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"
                      ],
                      "ProviderDefinitionId": "c444cda8-d9b5-45cc-a82d-fef28e08d55c",
                      "ContainerName": "test",
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
                      ]
                    }
                    """, content);

                data.ChangeType = VersionChangeType.Removed;
                await connector.StoreData(context, streamModel.Object, data);
                await WaitForFileToBeDeleted(fileSystemName, directoryName, client, path);
                await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
            }
            finally
            {
                await DeleteFileSystem(client, fileSystemName);
            }
        }

        [Fact]
        public async Task VerifyStoreData_Sync_WithStreamCacheAndJsonFormat()
        {
            await VerifyStoreData_Sync_WithStreamCache("JSON", AssertJsonResult);
        }

        [Fact]
        public async Task VerifyStoreData_Sync_WithStreamCacheAndCsvFormat()
        {
            await VerifyStoreData_Sync_WithStreamCache("csv", AssertCsvResult);
        }

        [Fact]
        public async Task VerifyStoreData_Sync_WithStreamCacheAndParquetFormat()
        {
            await VerifyStoreData_Sync_WithStreamCache("pArQuet", AssertParquetResult);
        }

        private async Task VerifyStoreData_Sync_WithStreamCache(string format, Func<DataLakeFileClient, Task> assertMethod)
        {
            var organizationId = Guid.NewGuid();
            var providerDefinitionId = Guid.Parse("c444cda8-d9b5-45cc-a82d-fef28e08d55c");

            var container = new WindsorContainer();

            container.Register(Component.For<ILogger<OrganizationDataStores>>()
                .Instance(new Mock<ILogger<OrganizationDataStores>>().Object));
            container.Register(Component.For<ILogger<ExecutionContext>>()
                .Instance(new Mock<ILogger<ExecutionContext>>().Object));

            var providerDefinition = new Mock<IRelationalDataStore<ProviderDefinition>>();
            providerDefinition.Setup(store => store.GetByIdAsync(It.IsAny<ExecutionContext>(), providerDefinitionId))
                .ReturnsAsync(new ProviderDefinition { IsEnabled = true, ProviderId = AzureDataLakeConstants.DataLakeProviderId });
            container.Register(Component.For<IRelationalDataStore<ProviderDefinition>>()
                .Instance(providerDefinition.Object));


            var streamId = Guid.NewGuid();
            var streamRepository = new Mock<IStreamRepository>();
            var streamModel = new StreamModel
            {
                Id = streamId,
                ConnectorProviderDefinitionId = providerDefinitionId,
                ContainerName = "test",
                Mode = StreamMode.Sync,
                ExportIncomingEdges = true,
                ExportOutgoingEdges = true,
                Status = StreamStatus.Started,
            };

            streamRepository.Setup(x => x.GetStream(streamId)).ReturnsAsync(streamModel);
            container.Register(Component.For<IStreamRepository>().Instance(streamRepository.Object));

            var cache = new Mock<InMemoryApplicationCache>(MockBehavior.Loose, container)
            {
                CallBase = true
            };
            container.Register(Component.For<IApplicationCache>().Instance(cache.Object));

            var systemConnectionStrings = new Mock<ISystemConnectionStrings>();
            container.Register(Component.For<ISystemConnectionStrings>().Instance(systemConnectionStrings.Object));
            container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
            container.Register(Component.For<IApplicationCache>().Instance(new Mock<IApplicationCache>().Object));

            var applicationContext = new ApplicationContext(container);
            var organizationDataShard = new Mock<IOrganizationDataShard>();
            systemConnectionStrings.Setup(x => x.SystemOrganizationDataShard).Returns(organizationDataShard.Object);
            var organization = new Organization(applicationContext, organizationId);
            var organizationRepository = new Mock<IOrganizationRepository>();
            organizationRepository.Setup(x => x.GetOrganization(It.IsAny<ExecutionContext>(), It.IsAny<Guid>()))
                .Returns(organization);
            container.Register(Component.For<IOrganizationRepository>().Instance(organizationRepository.Object));

            var logger = new Mock<ILogger>().Object;

            var context = new ExecutionContext(applicationContext, organization, logger);

            var azureDataLakeConstantsMock = new Mock<IAzureDataLakeConstants>();
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);
            azureDataLakeConstantsMock.Setup(x => x.ProviderId).Returns(AzureDataLakeConstants.DataLakeProviderId);

            var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
            Assert.NotNull(accountName);
            var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
            Assert.NotNull(accountKey);
            var streamCacheConnectionStringEncoded = Environment.GetEnvironmentVariable("ADL2_STREAMCACHE");
            var streamCacheConnectionString = Encoding.UTF8.GetString(Convert.FromBase64String(streamCacheConnectionStringEncoded));
            Console.WriteLine(streamCacheConnectionString);
            Assert.NotNull(accountKey);

            var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = $"xunit-{DateTime.Now.Ticks}";

            var connectorConnectionMock = new Mock<IConnectorConnectionV2>();
            connectorConnectionMock.Setup(x => x.Authentication).Returns(new Dictionary<string, object>()
            {
                { nameof(AzureDataLakeConstants.AccountName), accountName },
                { nameof(AzureDataLakeConstants.AccountKey), accountKey },
                { nameof(AzureDataLakeConstants.FileSystemName), fileSystemName },
                { nameof(AzureDataLakeConstants.DirectoryName), directoryName },
                { nameof(DataLakeConstants.IsStreamCacheEnabled), true },
                { nameof(DataLakeConstants.StreamCacheConnectionString), streamCacheConnectionString },
                { nameof(DataLakeConstants.OutputFormat), format },
                { nameof(DataLakeConstants.UseCurrentTimeForExport), true },
            });

            var azureDataLakeClient = new AzureDataLakeClient();
            var jobDataFactory = new Mock<AzureDataLakeJobDataFactory>();
            var connectorMock = new Mock<AzureDataLakeConnector>(
                new Mock<ILogger<AzureDataLakeConnector>>().Object,
                new AzureDataLakeClient(),
                azureDataLakeConstantsMock.Object,
                jobDataFactory.Object
            );

            jobDataFactory.Setup(x => x.GetConfiguration(It.IsAny<ExecutionContext>(), providerDefinitionId, It.IsAny<string>()))
                .ReturnsAsync(new AzureDataLakeConnectorJobData(connectorConnectionMock.Object.Authentication.ToDictionary(x => x.Key, x => x.Value)));
            var configurationRepository = new Mock<IConfigurationRepository>();
            configurationRepository.Setup(x => x.GetConfigurationById(It.IsAny<ExecutionContext>(), It.IsAny<Guid>()))
                .Returns(connectorConnectionMock.Object.Authentication.ToDictionary(x => x.Key, x => x.Value));

            container.Register(Component.For<IConfigurationRepository>().Instance(configurationRepository.Object));
            container.Register(Component.For<AzureDataLakeClient>().Instance(azureDataLakeClient));
            connectorMock.CallBase = true;

            var connector = connectorMock.Object;

            var dobInDateTime = new DateTime(2000, 01, 02, 03, 04, 05);
            var dobInDateTimeOffset = new DateTimeOffset(2000, 01, 02, 03, 04, 05, TimeSpan.FromMinutes(12 * 60 + 34));
            var data = new ConnectorEntityData(VersionChangeType.Added, StreamMode.EventStream,
                Guid.Parse("f55c66dc-7881-55c9-889f-344992e71cb8"),
                new ConnectorEntityPersistInfo("etypzcezkiehwq8vw4oqog==", 1), null,
                EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"),
                "/Person",
                new[]
                {
                    new ConnectorPropertyData("user.lastName", "Picard",
                        new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
                    new ConnectorPropertyData("user.age", "123",
                        new VocabularyKeyConnectorPropertyDataType(
                            new VocabularyKey("user.age", dataType: VocabularyKeyDataType.Integer)
                            {
                                Storage = VocabularyKeyStorage.Typed,
                            })),
                    new ConnectorPropertyData("user.dobInDateTime", dobInDateTime,
                        new VocabularyKeyConnectorPropertyDataType(
                            new VocabularyKey("user.dobInDateTime", dataType: VocabularyKeyDataType.DateTime)
                            {
                                Storage = VocabularyKeyStorage.Typed,
                            })),
                    new ConnectorPropertyData("user.dobInDateTimeOffset", dobInDateTimeOffset,
                        new VocabularyKeyConnectorPropertyDataType(
                            new VocabularyKey("user.dobInDateTimeOffset", dataType: VocabularyKeyDataType.DateTime)
                            {
                                Storage = VocabularyKeyStorage.Typed,
                            })),
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


            await connector.StoreData(context, streamModel, data);
            var client = new DataLakeServiceClient(new Uri($"https://{accountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(accountName, accountKey));

            try
            {
                await using var connection = new SqlConnection(streamCacheConnectionString);
                await connection.OpenAsync();
                var tableName = CacheTableHelper.GetCacheTableName(streamId);

                var getCountSql = $"SELECT COUNT(*) FROM [{tableName}]";
                var sqlCommand = new SqlCommand(getCountSql, connection)
                {
                    CommandType = CommandType.Text
                };
                var total = (int)await sqlCommand.ExecuteScalarAsync();

                Assert.Equal(1, total);

                var exportJob = new AzureDataLakeExportEntitiesJob(
                    applicationContext,
                    streamRepository.Object,
                    azureDataLakeClient,
                    azureDataLakeConstantsMock.Object,
                    jobDataFactory.Object);
                exportJob.Run(new Core.Jobs.JobArgs
                {
                    OrganizationId = organization.Id.ToString(),
                    Schedule = "* * * * *",
                    Message = streamId.ToString(),
                });


                var path = await WaitForFileToBeCreated(fileSystemName, directoryName, client);
                var fsClient = client.GetFileSystemClient(fileSystemName);
                var fileClient = fsClient.GetFileClient(path.Name);

                await assertMethod(fileClient);

                await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
            }
            finally
            {
                await DeleteFileSystem(client, fileSystemName);
                await DeleteTable(streamId, streamCacheConnectionString);
            }
        }

        private static async Task DeleteTable(Guid streamId, string streamCacheConnectionString)
        {
            await using var connection = new SqlConnection(streamCacheConnectionString);
            await connection.OpenAsync();
            var tableName = CacheTableHelper.GetCacheTableName(streamId);
            var deleteTableSql = $"""
                IF EXISTS (SELECT * FROM SYSOBJECTS WHERE NAME='{tableName}' AND XTYPE='U')
                ALTER TABLE dbo.[{tableName}]  SET ( SYSTEM_VERSIONING = Off )

                DROP TABLE IF EXISTS [{tableName}];
                DROP TABLE IF EXISTS [{tableName}_History];
                """;
            var command = new SqlCommand(deleteTableSql, connection)
            {
                CommandType = CommandType.Text
            };
            _ = await command.ExecuteNonQueryAsync();
        }

        private async Task AssertJsonResult(DataLakeFileClient fileClient)
        {
            using var streamReader = new StreamReader(fileClient.Read().Value.Content);
            var content = await streamReader.ReadToEndAsync();
            _testOutputHelper.WriteLine(content);
            Assert.Equal(
            $$"""
            [
              {
                "Id": "f55c66dc-7881-55c9-889f-344992e71cb8",
                "Codes": [
                  "/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"
                ],
                "ContainerName": "test",
                "EntityType": "/Person",
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
                "user.age": "123",
                "user.dobInDateTime": "2000-01-02T03:04:05",
                "user.dobInDateTimeOffset": "2000-01-02T03:04:05+12:34",
                "user.lastName": "Picard"
              }
            ]
            """, content);
        }

        private async Task AssertCsvResult(DataLakeFileClient fileClient)
        {
            using var streamReader = new StreamReader(fileClient.Read().Value.Content);
            var content = await streamReader.ReadToEndAsync();

            _testOutputHelper.WriteLine(content);
            Assert.Equal(
            $$$"""
            Id,Codes,ContainerName,EntityType,IncomingEdges,Name,OriginEntityCode,OutgoingEdges,PersistHash,PersistVersion,ProviderDefinitionId,user.age,user.dobInDateTime,user.dobInDateTimeOffset,user.lastName
            f55c66dc-7881-55c9-889f-344992e71cb8,"[""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0""]",test,/Person,"[{""FromReference"":{""Code"":{""Origin"":{""Code"":""Acceptance"",""Id"":null},""Value"":""7c5591cf-861a-4642-861d-3b02485854a0"",""Key"":""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",""Type"":{""IsEntityContainer"":false,""Root"":null,""Code"":""/Person""}},""Type"":{""IsEntityContainer"":false,""Root"":null,""Code"":""/Person""},""Name"":null,""Properties"":null,""PropertyCount"":null,""EntityId"":null,""IsEmpty"":false},""ToReference"":{""Code"":{""Origin"":{""Code"":""Somewhere"",""Id"":null},""Value"":""1234"",""Key"":""/EntityA#Somewhere:1234"",""Type"":{""IsEntityContainer"":false,""Root"":null,""Code"":""/EntityA""}},""Type"":{""IsEntityContainer"":false,""Root"":null,""Code"":""/EntityA""},""Name"":null,""Properties"":null,""PropertyCount"":null,""EntityId"":null,""IsEmpty"":false},""EdgeType"":{""Root"":null,""Code"":""/EntityA""},""HasProperties"":false,""Properties"":{},""CreationOptions"":0,""Weight"":null,""Version"":0}]",Jean Luc Picard,/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0,"[{""FromReference"":{""Code"":{""Origin"":{""Code"":""Somewhere"",""Id"":null},""Value"":""5678"",""Key"":""/EntityB#Somewhere:5678"",""Type"":{""IsEntityContainer"":false,""Root"":null,""Code"":""/EntityB""}},""Type"":{""IsEntityContainer"":false,""Root"":null,""Code"":""/EntityB""},""Name"":null,""Properties"":null,""PropertyCount"":null,""EntityId"":null,""IsEmpty"":false},""ToReference"":{""Code"":{""Origin"":{""Code"":""Acceptance"",""Id"":null},""Value"":""7c5591cf-861a-4642-861d-3b02485854a0"",""Key"":""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",""Type"":{""IsEntityContainer"":false,""Root"":null,""Code"":""/Person""}},""Type"":{""IsEntityContainer"":false,""Root"":null,""Code"":""/Person""},""Name"":null,""Properties"":null,""PropertyCount"":null,""EntityId"":null,""IsEmpty"":false},""EdgeType"":{""Root"":null,""Code"":""/EntityB""},""HasProperties"":false,""Properties"":{},""CreationOptions"":0,""Weight"":null,""Version"":0}]",etypzcezkiehwq8vw4oqog==,1,c444cda8-d9b5-45cc-a82d-fef28e08d55c,123,2000-01-02T03:04:05,2000-01-02T03:04:05+12:34,Picard

            """, content);
        }

        private async Task AssertParquetResult(DataLakeFileClient fileClient)
        {
            using var memoryStream = new MemoryStream();
            await fileClient.ReadToAsync(memoryStream);
            using var file = new ParquetFileReader(memoryStream);

            var sb = new StringBuilder();
            for (var rowGroup = 0; rowGroup < file.FileMetaData.NumRowGroups; ++rowGroup)
            {
                using var rowGroupReader = file.RowGroup(rowGroup);
                var groupNumRows = checked((int)rowGroupReader.MetaData.NumRows);

                var totalColumns = rowGroupReader.MetaData.NumColumns;

                for (var column = 0; column <totalColumns; ++column)
                {
                    var columnType = rowGroupReader.MetaData.Schema.Column(column).LogicalType.Type;
                    var columnReader = rowGroupReader.Column(column);

                    object value = getValue(groupNumRows, columnType, columnReader);
                    sb.AppendLine($"{rowGroupReader.MetaData.Schema.Column(column).Name} {value}");
                }
            }

            _testOutputHelper.WriteLine(sb.ToString());
            Assert.Equal(
            $$$"""
            Id f55c66dc-7881-55c9-889f-344992e71cb8
            Codes ["/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"]
            ContainerName test
            EntityType /Person
            IncomingEdges [{"FromReference":{"Code":{"Origin":{"Code":"Acceptance","Id":null},"Value":"7c5591cf-861a-4642-861d-3b02485854a0","Key":"/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0","Type":{"IsEntityContainer":false,"Root":null,"Code":"/Person"}},"Type":{"IsEntityContainer":false,"Root":null,"Code":"/Person"},"Name":null,"Properties":null,"PropertyCount":null,"EntityId":null,"IsEmpty":false},"ToReference":{"Code":{"Origin":{"Code":"Somewhere","Id":null},"Value":"1234","Key":"/EntityA#Somewhere:1234","Type":{"IsEntityContainer":false,"Root":null,"Code":"/EntityA"}},"Type":{"IsEntityContainer":false,"Root":null,"Code":"/EntityA"},"Name":null,"Properties":null,"PropertyCount":null,"EntityId":null,"IsEmpty":false},"EdgeType":{"Root":null,"Code":"/EntityA"},"HasProperties":false,"Properties":{},"CreationOptions":0,"Weight":null,"Version":0}]
            Name Jean Luc Picard
            OriginEntityCode /Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0
            OutgoingEdges [{"FromReference":{"Code":{"Origin":{"Code":"Somewhere","Id":null},"Value":"5678","Key":"/EntityB#Somewhere:5678","Type":{"IsEntityContainer":false,"Root":null,"Code":"/EntityB"}},"Type":{"IsEntityContainer":false,"Root":null,"Code":"/EntityB"},"Name":null,"Properties":null,"PropertyCount":null,"EntityId":null,"IsEmpty":false},"ToReference":{"Code":{"Origin":{"Code":"Acceptance","Id":null},"Value":"7c5591cf-861a-4642-861d-3b02485854a0","Key":"/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0","Type":{"IsEntityContainer":false,"Root":null,"Code":"/Person"}},"Type":{"IsEntityContainer":false,"Root":null,"Code":"/Person"},"Name":null,"Properties":null,"PropertyCount":null,"EntityId":null,"IsEmpty":false},"EdgeType":{"Root":null,"Code":"/EntityB"},"HasProperties":false,"Properties":{},"CreationOptions":0,"Weight":null,"Version":0}]
            PersistHash etypzcezkiehwq8vw4oqog==
            PersistVersion 1
            ProviderDefinitionId c444cda8-d9b5-45cc-a82d-fef28e08d55c
            user.age 123
            user.dobInDateTime 2000-01-02T03:04:05
            user.dobInDateTimeOffset 2000-01-02T03:04:05+12:34
            user.lastName Picard

            """, sb.ToString());

            object getValue(int groupNumRows, LogicalTypeEnum columnType, ColumnReader columnReader)
            {
                switch (columnType)
                {
                    case LogicalTypeEnum.Uuid:
                        return columnReader.LogicalReader<Guid?>().ReadAll(groupNumRows).Single();
                    case LogicalTypeEnum.Int:
                        return columnReader.LogicalReader<int?>().ReadAll(groupNumRows).Single();
                    case LogicalTypeEnum.String:
                        return columnReader.LogicalReader<string>().ReadAll(groupNumRows).Single();
                    default:
                        throw new NotSupportedException($"Type {columnType} not supported.");
                }
            }
        }

        

        private static async Task WaitForFileToBeDeleted(string fileSystemName, string directoryName, DataLakeServiceClient client, PathItem path)
        {
            var d = DateTime.Now;
            while (true)
            {
                if (DateTime.Now > d.AddSeconds(30))
                {
                    throw new TimeoutException("Timeout waiting for file to be deleted");
                }

                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }

                var fsClient = client.GetFileSystemClient(fileSystemName);

                var paths = fsClient.GetPaths(recursive: true)
                    .Where(p => p.IsDirectory == false)
                    .Where(p => p.Name.Contains(directoryName))
                    .Select(p => p.Name)
                    .ToArray();

                if (paths.Contains(path.Name))
                {
                    await Task.Delay(1000);
                    continue;
                }

                break;
            }
        }

        private static async Task DeleteFileSystem(DataLakeServiceClient client, string fileSystemName)
        {
            if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
            {
                return;
            }
            var fsClient = client.GetFileSystemClient(fileSystemName);
            await fsClient.DeleteAsync();
        }

        private static async Task<PathItem> WaitForFileToBeCreated(string fileSystemName, string directoryName, DataLakeServiceClient client)
        {
            PathItem path;
            var d = DateTime.Now;
            while (true)
            {
                if (DateTime.Now > d.AddSeconds(30))
                {
                    throw new TimeoutException();
                }

                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }

                var fsClient = client.GetFileSystemClient(fileSystemName);

                var paths = fsClient.GetPaths(recursive: true)
                    .Where(p => p.IsDirectory == false)
                    .Where(p => p.Name.Contains(directoryName))
                    .ToArray();

                if (paths.Length == 0)
                {
                    await Task.Delay(1000);
                    continue;
                }

                path = paths.Single();

                if (path.ContentLength > 0)
                {
                    break;
                }
            }

            return path;
        }
    }
}
