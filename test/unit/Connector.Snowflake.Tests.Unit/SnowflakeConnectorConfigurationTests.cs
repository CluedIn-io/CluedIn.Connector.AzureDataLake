using System.Collections.Generic;

using CluedIn.Connector.Snowflake;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Unit;

public class SnowflakeConnectorConfigurationTests
{
    private static Dictionary<string, object> CreateConfigurationValues()
    {
        return new Dictionary<string, object>
        {
            [SnowflakeConfigurationConstants.Account] = "qs30799",
            [SnowflakeConfigurationConstants.User] = "cluedin_svc",
            [SnowflakeConfigurationConstants.PrivateKey] = "-----BEGIN PRIVATE KEY-----\nMII...\n-----END PRIVATE KEY-----",
            [SnowflakeConfigurationConstants.Database] = "SNOWFLAKE_LEARNING_DB",
            [SnowflakeConfigurationConstants.Schema] = "TESTSCHEMA",
            [SnowflakeConfigurationConstants.Warehouse] = "COMPUTE_WH",
            [SnowflakeConfigurationConstants.Role] = "ACCOUNTADMIN",
            [SnowflakeConfigurationConstants.TableName] = "MYTESTTABLE",
        };
    }

    [Fact]
    public void Properties_MapFromConfigurationDictionary()
    {
        var configuration = new SnowflakeConnectorConfiguration(CreateConfigurationValues());

        Assert.Equal("qs30799", configuration.Account);
        Assert.Equal("cluedin_svc", configuration.User);
        Assert.Equal("SNOWFLAKE_LEARNING_DB", configuration.Database);
        Assert.Equal("TESTSCHEMA", configuration.Schema);
        Assert.Equal("COMPUTE_WH", configuration.Warehouse);
        Assert.Equal("ACCOUNTADMIN", configuration.Role);
        Assert.Equal("MYTESTTABLE", configuration.TableName);
    }

    [Fact]
    public void DeltaAndStreamCacheAreAlwaysEnabled_RegardlessOfConfiguration()
    {
        var configuration = new SnowflakeConnectorConfiguration(CreateConfigurationValues());

        Assert.True(configuration.IsStreamCacheEnabled);
        Assert.True(configuration.IsDeltaMode);
    }

    [Fact]
    public void SoftDeleteIsNotOverridden_InheritsBaseDefaultOfTrue()
    {
        var configuration = new SnowflakeConnectorConfiguration(CreateConfigurationValues());

        Assert.True(configuration.IsSoftDelete);
    }

    [Fact]
    public void TransientTableAndPipeNames_AreDerivedFromTargetTableNameAndContainerName()
    {
        var configuration = new SnowflakeConnectorConfiguration(CreateConfigurationValues(), "my-stream-container");

        Assert.Equal("MYTESTTABLE__CLUEDIN_TRANSIENT__MY_STREAM_CONTAINER", configuration.TransientTableName);
        Assert.Equal("MYTESTTABLE__CLUEDIN_PIPE__MY_STREAM_CONTAINER", configuration.PipeName);
    }

    // Two streams configured with the same target TableName must not collide on a shared
    // transient table/pipe - see SnowflakeConnectorConfiguration.TransientTableName/PipeName.
    [Fact]
    public void TransientTableAndPipeNames_DifferByContainerName_ForStreamsSharingATargetTable()
    {
        var firstStream = new SnowflakeConnectorConfiguration(CreateConfigurationValues(), "stream-one");
        var secondStream = new SnowflakeConnectorConfiguration(CreateConfigurationValues(), "stream-two");

        Assert.NotEqual(firstStream.TransientTableName, secondStream.TransientTableName);
        Assert.NotEqual(firstStream.PipeName, secondStream.PipeName);
    }

    [Fact]
    public void RootDirectoryPath_IsDatabaseDotSchema()
    {
        var configuration = new SnowflakeConnectorConfiguration(CreateConfigurationValues());

        Assert.Equal("SNOWFLAKE_LEARNING_DB.TESTSCHEMA", configuration.RootDirectoryPath);
    }
}
