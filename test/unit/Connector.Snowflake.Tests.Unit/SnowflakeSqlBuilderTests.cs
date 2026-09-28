using CluedIn.Connector.Snowflake.Connector;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Unit;

public class SnowflakeSqlBuilderTests
{
    private const string Database = "SNOWFLAKE_LEARNING_DB";
    private const string Schema = "TESTSCHEMA";
    private const string TransientTableName = "MYTESTTABLE__CLUEDIN_TRANSIENT";
    private const string PipeName = "MYTESTTABLE__CLUEDIN_PIPE";
    private const string TargetTableName = "MYTESTTABLE";

    [Fact]
    public void QualifiedName_WrapsEachPartInDoubleQuotes()
    {
        var qualified = SnowflakeSqlBuilder.QualifiedName(Database, Schema, TargetTableName);

        Assert.Equal("\"SNOWFLAKE_LEARNING_DB\".\"TESTSCHEMA\".\"MYTESTTABLE\"", qualified);
    }

    [Fact]
    public void CreateTransientTableIfNotExists_DeclaresExpectedColumns()
    {
        var sql = SnowflakeSqlBuilder.CreateTransientTableIfNotExists(Database, Schema, TransientTableName);

        Assert.Contains("CREATE TRANSIENT TABLE IF NOT EXISTS", sql);
        Assert.Contains(SnowflakeSqlBuilder.QualifiedName(Database, Schema, TransientTableName), sql);
        Assert.Contains($"{TransientTableColumns.EntityId} VARCHAR", sql);
        Assert.Contains($"{TransientTableColumns.ChangeType} VARCHAR", sql);
        Assert.Contains($"{TransientTableColumns.PersistVersion} NUMBER", sql);
        Assert.Contains($"{TransientTableColumns.RowData} VARIANT", sql);
    }

    [Fact]
    public void CreatePipeIfNotExists_BindsPipeToTransientTableAsStreamingSource()
    {
        var sql = SnowflakeSqlBuilder.CreatePipeIfNotExists(Database, Schema, PipeName, TransientTableName);

        Assert.Contains("CREATE PIPE IF NOT EXISTS", sql);
        Assert.Contains(SnowflakeSqlBuilder.QualifiedName(Database, Schema, PipeName), sql);
        Assert.Contains($"COPY INTO {SnowflakeSqlBuilder.QualifiedName(Database, Schema, TransientTableName)}", sql);
        Assert.Contains("DATA_SOURCE(TYPE => 'STREAMING')", sql);
    }

    [Fact]
    public void TruncateTransientTable_TargetsTheTransientTableOnly()
    {
        var sql = SnowflakeSqlBuilder.TruncateTransientTable(Database, Schema, TransientTableName);

        Assert.Equal($"TRUNCATE TABLE IF EXISTS {SnowflakeSqlBuilder.QualifiedName(Database, Schema, TransientTableName)}", sql);
    }

    [Fact]
    public void MergeTransientIntoTarget_DedupesByLatestPersistVersionPerEntity()
    {
        var sql = SnowflakeSqlBuilder.MergeTransientIntoTarget(Database, Schema, TransientTableName, TargetTableName);

        Assert.Contains($"ROW_NUMBER() OVER (PARTITION BY {TransientTableColumns.EntityId} ORDER BY {TransientTableColumns.PersistVersion} DESC) AS RN", sql);
        Assert.Contains("WHERE RN = 1", sql);
    }

    [Fact]
    public void MergeTransientIntoTarget_AppliesDeleteUpdateInsertByChangeType()
    {
        var sql = SnowflakeSqlBuilder.MergeTransientIntoTarget(Database, Schema, TransientTableName, TargetTableName);

        Assert.Contains($"WHEN MATCHED AND source.{TransientTableColumns.ChangeType} = 'Removed' THEN DELETE", sql);
        Assert.Contains($"WHEN MATCHED THEN UPDATE SET target.DATA = source.{TransientTableColumns.RowData}", sql);
        Assert.Contains($"WHEN NOT MATCHED AND source.{TransientTableColumns.ChangeType} != 'Removed' THEN INSERT", sql);
    }

    [Fact]
    public void MergeTransientIntoTarget_JoinsOnEntityIdAgainstTargetId()
    {
        var sql = SnowflakeSqlBuilder.MergeTransientIntoTarget(Database, Schema, TransientTableName, TargetTableName);

        Assert.Contains($"ON target.ID = source.{TransientTableColumns.EntityId}", sql);
        Assert.Contains($"MERGE INTO {SnowflakeSqlBuilder.QualifiedName(Database, Schema, TargetTableName)} AS target", sql);
    }
}
