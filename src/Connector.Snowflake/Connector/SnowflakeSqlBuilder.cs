namespace CluedIn.Connector.Snowflake.Connector;

// Pure SQL text generation, kept separate from SnowflakeExportEntitiesJob so it can be unit
// tested without a live Snowflake connection.
internal static class SnowflakeSqlBuilder
{
    public static string QualifiedName(string database, string schema, string objectName)
    {
        return $"\"{database}\".\"{schema}\".\"{objectName}\"";
    }

    public static string CreateTransientTableIfNotExists(string database, string schema, string transientTableName)
    {
        var qualified = QualifiedName(database, schema, transientTableName);
        return $"""
            CREATE TRANSIENT TABLE IF NOT EXISTS {qualified} (
                {TransientTableColumns.EntityId} VARCHAR,
                {TransientTableColumns.ChangeType} VARCHAR,
                {TransientTableColumns.PersistVersion} NUMBER,
                {TransientTableColumns.RowData} VARIANT
            )
            """;
    }

    public static string CreatePipeIfNotExists(string database, string schema, string pipeName, string transientTableName)
    {
        var qualifiedPipe = QualifiedName(database, schema, pipeName);
        var qualifiedTable = QualifiedName(database, schema, transientTableName);
        return $"""
            CREATE PIPE IF NOT EXISTS {qualifiedPipe}
            AS COPY INTO {qualifiedTable}
            FROM TABLE (DATA_SOURCE(TYPE => 'STREAMING'))
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """;
    }

    public static string TruncateTransientTable(string database, string schema, string transientTableName)
    {
        return $"TRUNCATE TABLE IF EXISTS {QualifiedName(database, schema, transientTableName)}";
    }

    // Assumes the target table has an ID column (matching ENTITY_ID) and a DATA VARIANT
    // column - see docs/snowflake-connector-plan.md for why the transient table (and by
    // extension the expected target shape) is schema-independent rather than one column
    // per CluedIn property.
    public static string MergeTransientIntoTarget(string database, string schema, string transientTableName, string targetTableName)
    {
        var qualifiedTarget = QualifiedName(database, schema, targetTableName);
        var qualifiedTransient = QualifiedName(database, schema, transientTableName);
        return $"""
            MERGE INTO {qualifiedTarget} AS target
            USING (
                SELECT {TransientTableColumns.EntityId}, {TransientTableColumns.ChangeType}, {TransientTableColumns.RowData}
                FROM (
                    SELECT
                        {TransientTableColumns.EntityId},
                        {TransientTableColumns.ChangeType},
                        {TransientTableColumns.RowData},
                        ROW_NUMBER() OVER (PARTITION BY {TransientTableColumns.EntityId} ORDER BY {TransientTableColumns.PersistVersion} DESC) AS RN
                    FROM {qualifiedTransient}
                )
                WHERE RN = 1
            ) AS source
            ON target.ID = source.{TransientTableColumns.EntityId}
            WHEN MATCHED AND source.{TransientTableColumns.ChangeType} = 'Removed' THEN DELETE
            WHEN MATCHED THEN UPDATE SET target.DATA = source.{TransientTableColumns.RowData}
            WHEN NOT MATCHED AND source.{TransientTableColumns.ChangeType} != 'Removed' THEN INSERT (ID, DATA) VALUES (source.{TransientTableColumns.EntityId}, source.{TransientTableColumns.RowData})
            """;
    }
}
