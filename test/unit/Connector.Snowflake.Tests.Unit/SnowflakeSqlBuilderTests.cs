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

    // Database/schema/table names come from connector configuration - an embedded double
    // quote must be doubled (the standard SQL escape) rather than left to break out of the
    // quoted identifier and inject arbitrary SQL into the generated statement.
    [Fact]
    public void QualifiedName_EscapesEmbeddedDoubleQuotes()
    {
        var qualified = SnowflakeSqlBuilder.QualifiedName("DB\"; DROP TABLE X; --", "SCHEMA", "TABLE");

        Assert.Equal("\"DB\"\"; DROP TABLE X; --\".\"SCHEMA\".\"TABLE\"", qualified);
    }

    // TableName's resolved value is reused as the transient table/pipe name (see
    // SnowflakeConnectorConfiguration.GetTransientTableName/GetPipeName), so it must stay
    // the same across export runs for a given stream - {DataTime} is the only pattern
    // variable that wouldn't, so it's rejected outright.
    [Theory]
    [InlineData("{DataTime}", true)]
    [InlineData("{DataTime:yyyyMMdd}", true)]
    [InlineData("{datatime}", true)]
    [InlineData("PREFIX_{DataTime}_SUFFIX", true)]
    [InlineData("{StreamId}", false)]
    [InlineData("{ContainerName}_TABLE", false)]
    [InlineData("{OutputFormat}", false)]
    [InlineData("MYTESTTABLE", false)]
    [InlineData(null, false)]
    [InlineData("", false)]
    public void ContainsDataTimePatternVariable_DetectsDataTimeVariableOnly(string tableNamePattern, bool expected)
    {
        Assert.Equal(expected, SnowflakeSqlBuilder.ContainsDataTimePatternVariable(tableNamePattern));
    }

    [Theory]
    [InlineData("Id", "ID")]
    [InlineData("__ChangeType__", "__CHANGETYPE__")]
    [InlineData("PersistVersion", "PERSISTVERSION")]
    [InlineData("user.email", "USER_EMAIL")]
    [InlineData("2fa_enabled", "_2FA_ENABLED")]
    public void SanitizeColumnName_UpperCasesAndReplacesNonAlphanumerics(string fieldName, string expected)
    {
        Assert.Equal(expected, SnowflakeSqlBuilder.SanitizeColumnName(fieldName));
    }

    // SanitizeColumnName isn't injective - distinct field names can fold to the same
    // column. EnsureNoColumnNameCollisions is the guard against silently generating a table
    // with duplicate columns (or a writer that drops one of the colliding values).
    [Fact]
    public void EnsureNoColumnNameCollisions_ThrowsWhenDistinctFieldsSanitizeToSameColumn()
    {
        var fields = new[] { "user.email", "user-email" };

        var ex = Assert.Throws<System.InvalidOperationException>(() => SnowflakeSqlBuilder.EnsureNoColumnNameCollisions(fields));
        Assert.Contains("USER_EMAIL", ex.Message);
    }

    [Fact]
    public void EnsureNoColumnNameCollisions_DoesNotThrowForUniqueFields()
    {
        SnowflakeSqlBuilder.EnsureNoColumnNameCollisions(FieldNames);
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

    // The COMMENT clause is a no-op when IF NOT EXISTS finds the table already there, so it
    // doubles as an ownership marker: SnowflakeConnector.ArchiveContainer reads it back
    // (via ShowTablesLikeInSchema) to decide whether this connector is safe to drop the
    // target table on archive.
    [Fact]
    public void CreateTargetTableIfNotExists_StampsOwnershipComment()
    {
        var sql = SnowflakeSqlBuilder.CreateTargetTableIfNotExists(Database, Schema, TargetTableName, FieldNames);

        Assert.Contains($"COMMENT = '{SnowflakeSqlBuilder.OwnedTableComment}'", sql);
    }

    [Fact]
    public void ShowTablesLikeInSchema_FiltersByExactNameWithinSchema()
    {
        var sql = SnowflakeSqlBuilder.ShowTablesLikeInSchema(Database, Schema, TargetTableName);

        Assert.Equal($"SHOW TABLES LIKE '{TargetTableName}' IN SCHEMA \"{Database}\".\"{Schema}\"", sql);
    }

    [Fact]
    public void ShowPipesLikeInSchema_FiltersByExactNameWithinSchema()
    {
        var sql = SnowflakeSqlBuilder.ShowPipesLikeInSchema(Database, Schema, PipeName);

        Assert.Equal("SHOW PIPES LIKE 'MYTESTTABLE\\_\\_CLUEDIN\\_PIPE' IN SCHEMA \"SNOWFLAKE_LEARNING_DB\".\"TESTSCHEMA\"", sql);
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

        // '_' is a LIKE wildcard - it must be escaped so this only matches a warehouse
        // literally named "COMPUTE_WH", not e.g. "COMPUTEXWH" too.
        Assert.Equal("SHOW WAREHOUSES LIKE 'COMPUTE\\_WH'", sql);
    }

    [Fact]
    public void ShowDatabases_FiltersByExactName()
    {
        var sql = SnowflakeSqlBuilder.ShowDatabases(Database);

        Assert.Equal("SHOW DATABASES LIKE 'SNOWFLAKE\\_LEARNING\\_DB'", sql);
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

        Assert.Equal("SHOW WAREHOUSES LIKE 'O''BRIEN\\_WH'", sql);
    }

    [Theory]
    [InlineData("_", "\\_")]
    [InlineData("%", "\\%")]
    [InlineData("\\", "\\\\")]
    [InlineData("COMPUTE_WH%TEST", "COMPUTE\\_WH\\%TEST")]
    public void ShowWarehouses_EscapesLikeWildcards(string warehouseName, string expectedEscaped)
    {
        var sql = SnowflakeSqlBuilder.ShowWarehouses(warehouseName);

        Assert.Equal($"SHOW WAREHOUSES LIKE '{expectedEscaped}'", sql);
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
