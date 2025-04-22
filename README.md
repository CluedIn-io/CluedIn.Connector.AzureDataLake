# CluedIn.Connector.AzureDataLake

# About CluedIn
CluedIn is the Cloud-native Master Data Management Platform that brings data teams together enabling them to deliver the foundation of high-quality, trusted data that empowers everyone to make a difference. 

We're different because we use enhanced data management techniques like [Graph](https://www.cluedin.com/graph-versus-relational-databases-which-is-best) and [Zero Upfront Modelling](https://www.cluedin.com/upfront-versus-dynamic-data-modelling) to accelerate the time taken to prepare data to deliver insight by as much as 80%. Installed in as little as 20 minutes from the [Azure Marketplace](https://azuremarketplace.microsoft.com/en-gb/marketplace/apps/cluedin.azure_cluedin?tab=Overview), CluedIn is fully integrated with [Microsoft Purview](https://www.cluedin.com/product/microsoft-purview-mdm-integration?hsCtaTracking=461021ab-7a38-41a3-93dd-cfe2325dfd35%7Cb835efc0-e9b7-4385-a1b6-75cb7632527b) and the full [Microsoft Fabric](https://www.cluedin.com/microsoft-fabric) suite, making it the preferred choice for [Azure customers](https://www.cluedin.com/microsoft-intelligent-data-platform). 

To learn more about CluedIn, [contact the team](https://www.cluedin.com/discovery-call) today.

[https://www.cluedin.com](https://www.cluedin.com)


## Development

### Parquet File Output

Microsoft support for Parquet format varies across products:
1. GUID. One Lake and Open Mirroring doesn't support GUID type. It has to be serialized as a string
1. Array of Strings. 
   1. One Lake will not have an error but preview and SQL Analytics endpoint doesn't work
   1. Open Mirroring will have an error. Thus it cannot be added to the export
   1. For One Lake and Azure Data Lake, extra columns are added to facilitate usecases where the values are needed. It is in the form of X_String
1. Characters in columns. One Lake and Open Mirroring only support alphanumeric and space. No other characters are supported, including dot for vocabulary keys.
Thus they have to be escaped.

The table below shows what the connectors will output.

|       Item      | Array of String | JSON String | GUID as String | Escape Vocabulary Keys |
|:---------------:|:---------------:|:-----------:|:--------------:|:----------------------:|
| Azure Data Lake |       YES       |     YES     |    OPTIONAL    |        OPTIONAL        |
|     One Lake    |       YES       |     YES     |       YES      |           YES          |
|  Open Mirroring |        NO       |     YES     |       YES      |           YES          |