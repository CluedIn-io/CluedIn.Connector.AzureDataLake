using System.Collections.Generic;

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

    private static readonly IReadOnlyList<string> FieldNames = new[] { "Id", "__ChangeType__", "PersistVersion", "user.email" };

    [Fact]
    public void QualifiedName_WrapsEachPartInDoubleQuotes()
    {
        var qualified = SnowflakeSqlBuilder.QualifiedName(Database, Schema, TargetTableName);

        Assert.Equal("\"SNOWFLAKE_LEARNING_DB\".\"TESTSCHEMA\".\"MYTESTTABLE\"", qualified);
    }

    [Theory]
    [InlineData("Id", "ID")]
    [InlineData("__ChangeType__", "__CHANGETYPE__")]
    [InlineData("PersistVersion", "PERSISTVERSION")]
    [InlineData("user.email", "USER_EMAIL")]
    public void SanitizeColumnName_UpperCasesAndReplacesNonAlphanumerics(string fieldName, string expected)
    {
        Assert.Equal(expected, SnowflakeSqlBuilder.SanitizeColumnName(fieldName));
    }

    [Fact]
    public void CreateTransientTableIfNotExists_DeclaresOneColumnPerFieldName()
    {
        var sql = SnowflakeSqlBuilder.CreateTransientTableIfNotExists(Database, Schema, TransientTableName, FieldNames);

        Assert.Contains("CREATE TRANSIENT TABLE IF NOT EXISTS", sql);
        Assert.Contains(SnowflakeSqlBuilder.QualifiedName(Database, Schema, TransientTableName), sql);
        Assert.Contains("ID VARCHAR", sql);
        Assert.Contains("__CHANGETYPE__ VARCHAR", sql);
        Assert.Contains("PERSISTVERSION NUMBER", sql);
        Assert.Contains("USER_EMAIL VARCHAR", sql);
        Assert.DoesNotContain("VARIANT", sql);
    }

    [Fact]
    public void GetAddMissingColumnsStatements_ReturnsOneAlterStatementPerFieldName()
    {
        var statements = SnowflakeSqlBuilder.GetAddMissingColumnsStatements(Database, Schema, TransientTableName, FieldNames);

        var statementList = new List<string>(statements);
        Assert.Equal(FieldNames.Count, statementList.Count);
        Assert.Contains(statementList, s => s.Contains("ADD COLUMN IF NOT EXISTS ID VARCHAR"));
        Assert.Contains(statementList, s => s.Contains("ADD COLUMN IF NOT EXISTS PERSISTVERSION NUMBER"));
        Assert.All(statementList, s => Assert.Contains(SnowflakeSqlBuilder.QualifiedName(Database, Schema, TransientTableName), s));
    }

    [Fact]
    public void GetAddMissingColumnsStatements_TargetsWhicheverTableNameIsPassedIn()
    {
        var statements = SnowflakeSqlBuilder.GetAddMissingColumnsStatements(Database, Schema, TargetTableName, FieldNames);

        var statementList = new List<string>(statements);
        Assert.All(statementList, s => Assert.Contains(SnowflakeSqlBuilder.QualifiedName(Database, Schema, TargetTableName), s));
        Assert.All(statementList, s => Assert.DoesNotContain(TransientTableName, s));
    }

    [Fact]
    public void CreateTargetTableIfNotExists_DeclaresOneColumnPerFieldNameAsARegularTable()
    {
        var sql = SnowflakeSqlBuilder.CreateTargetTableIfNotExists(Database, Schema, TargetTableName, FieldNames);

        Assert.Contains("CREATE TABLE IF NOT EXISTS", sql);
        Assert.DoesNotContain("TRANSIENT", sql);
        Assert.Contains(SnowflakeSqlBuilder.QualifiedName(Database, Schema, TargetTableName), sql);
        Assert.Contains("ID VARCHAR", sql);
        Assert.Contains("__CHANGETYPE__ VARCHAR", sql);
        Assert.Contains("PERSISTVERSION NUMBER", sql);
        Assert.Contains("USER_EMAIL VARCHAR", sql);
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
    public void DropTableIfExists_TargetsWhicheverTableNameIsPassedIn()
    {
        var sql = SnowflakeSqlBuilder.DropTableIfExists(Database, Schema, TargetTableName);

        Assert.Equal($"DROP TABLE IF EXISTS {SnowflakeSqlBuilder.QualifiedName(Database, Schema, TargetTableName)}", sql);
    }

    [Fact]
    public void DropPipeIfExists_TargetsThePipe()
    {
        var sql = SnowflakeSqlBuilder.DropPipeIfExists(Database, Schema, PipeName);

        Assert.Equal($"DROP PIPE IF EXISTS {SnowflakeSqlBuilder.QualifiedName(Database, Schema, PipeName)}", sql);
    }

    [Fact]
    public void ShowWarehouses_FiltersByExactName()
    {
        var sql = SnowflakeSqlBuilder.ShowWarehouses("COMPUTE_WH");

        Assert.Equal("SHOW WAREHOUSES LIKE 'COMPUTE_WH'", sql);
    }

    [Fact]
    public void ShowDatabases_FiltersByExactName()
    {
        var sql = SnowflakeSqlBuilder.ShowDatabases(Database);

        Assert.Equal($"SHOW DATABASES LIKE '{Database}'", sql);
    }

    [Fact]
    public void ShowSchemasInDatabase_FiltersByExactNameWithinDatabase()
    {
        var sql = SnowflakeSqlBuilder.ShowSchemasInDatabase(Database, Schema);

        Assert.Equal($"SHOW SCHEMAS LIKE '{Schema}' IN DATABASE \"{Database}\"", sql);
    }

    [Fact]
    public void ShowWarehouses_EscapesSingleQuotesInTheName()
    {
        var sql = SnowflakeSqlBuilder.ShowWarehouses("O'BRIEN_WH");

        Assert.Equal("SHOW WAREHOUSES LIKE 'O''BRIEN_WH'", sql);
    }

    [Fact]
    public void MergeTransientIntoTarget_DedupesByLatestPersistVersionPerEntity()
    {
        var sql = SnowflakeSqlBuilder.MergeTransientIntoTarget(Database, Schema, TransientTableName, TargetTableName, FieldNames);

        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY ID ORDER BY PERSISTVERSION DESC) AS RN", sql);
        Assert.Contains("WHERE RN = 1", sql);
    }

    [Fact]
    public void MergeTransientIntoTarget_AppliesDeleteUpdateInsertByChangeType()
    {
        var sql = SnowflakeSqlBuilder.MergeTransientIntoTarget(Database, Schema, TransientTableName, TargetTableName, FieldNames);

        Assert.Contains("WHEN MATCHED AND source.__CHANGETYPE__ = 'Removed' THEN DELETE", sql);
        Assert.Contains("WHEN MATCHED THEN UPDATE SET", sql);
        Assert.Contains("target.USER_EMAIL = source.USER_EMAIL", sql);
        Assert.Contains("WHEN NOT MATCHED AND source.__CHANGETYPE__ != 'Removed' THEN INSERT", sql);
    }

    // __ChangeType__ is transient-only bookkeeping the MERGE reads to decide which action to
    // take - it's read from the transient table (source.__CHANGETYPE__ in the WHEN clauses
    // above), but must never be written into the target table as a persisted column/value.
    [Fact]
    public void MergeTransientIntoTarget_ExcludesChangeTypeFromTargetInsertAndUpdate()
    {
        var sql = SnowflakeSqlBuilder.MergeTransientIntoTarget(Database, Schema, TransientTableName, TargetTableName, FieldNames);

        Assert.DoesNotContain("target.__CHANGETYPE__", sql);
        Assert.DoesNotContain("INSERT (ID, __CHANGETYPE__", sql);
    }

    [Fact]
    public void MergeTransientIntoTarget_DoesNotIncludeIdColumnInUpdateSet()
    {
        var sql = SnowflakeSqlBuilder.MergeTransientIntoTarget(Database, Schema, TransientTableName, TargetTableName, FieldNames);

        Assert.DoesNotContain("target.ID = source.ID,", sql);
        Assert.DoesNotContain("SET target.ID = source.ID", sql);
    }

    [Fact]
    public void MergeTransientIntoTarget_JoinsOnIdAgainstTargetId()
    {
        var sql = SnowflakeSqlBuilder.MergeTransientIntoTarget(Database, Schema, TransientTableName, TargetTableName, FieldNames);

        Assert.Contains("ON target.ID = source.ID", sql);
        Assert.Contains($"MERGE INTO {SnowflakeSqlBuilder.QualifiedName(Database, Schema, TargetTableName)} AS target", sql);
    }

    [Fact]
    public void MergeTransientIntoTarget_InsertsAllColumnsIncludingIdButExcludingChangeType()
    {
        var sql = SnowflakeSqlBuilder.MergeTransientIntoTarget(Database, Schema, TransientTableName, TargetTableName, FieldNames);

        Assert.Contains("INSERT (ID, PERSISTVERSION, USER_EMAIL) VALUES (source.ID, source.PERSISTVERSION, source.USER_EMAIL)", sql);
    }
}
