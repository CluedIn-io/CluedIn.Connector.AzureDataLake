using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

using CluedIn.Connector.FileStorage.Common;

namespace CluedIn.Connector.Snowflake.Connector;

// Pure SQL text generation, kept separate from SnowflakeExportEntitiesJob so it can be unit
// tested without a live Snowflake connection.
//
// The transient table (and by extension the target table it's merged into) has one column
// per CluedIn property - matching how the old CluedIn.Connector.Snowflake repo's
// SnowflakeClient.BuildCreateContainerSql/BuildStoreDataSql shape the table (one VARCHAR
// column per field), not a single VARIANT blob column. Column names are derived from the
// same field names the SQL Server cache table already uses (see
// StorageConnectorBase.WriteToCacheTable), sanitized and upper-cased so they match
// Snowflake's default (unquoted) identifier folding regardless of what the source field
// name looked like.
internal static class SnowflakeSqlBuilder
{
    private static readonly Regex NonAlphaNumericRegex = new("[^a-zA-Z0-9_]", RegexOptions.Compiled);

    // Marks a target table as created (and therefore owned) by this connector, so
    // SnowflakeConnector.ArchiveContainer can tell it apart from a table the user pointed
    // the connector at that already existed - see CreateTargetTableIfNotExists (the COMMENT
    // clause only takes effect when CREATE TABLE IF NOT EXISTS actually creates the table,
    // never when it already existed) and ShowTablesLikeInSchema (used to read the comment
    // back before deciding whether to drop).
    public const string OwnedTableComment = "Created and owned by the CluedIn Snowflake connector - safe to drop when its stream is archived.";

    // Database/schema/table names come from connector configuration - a name containing a
    // double quote would otherwise break out of the quoted identifier and let arbitrary SQL
    // be injected into every generated DDL/MERGE statement. Doubling an embedded quote is
    // the standard Snowflake (and ANSI SQL) escape for a quoted identifier.
    public static string EscapeIdentifier(string identifier)
    {
        return identifier?.Replace("\"", "\"\"");
    }

    public static string QualifiedName(string database, string schema, string objectName)
    {
        return $"\"{EscapeIdentifier(database)}\".\"{EscapeIdentifier(schema)}\".\"{EscapeIdentifier(objectName)}\"";
    }

    // Snowflake identifiers fold to upper-case unless quoted; sanitizing and upper-casing
    // here up front means every caller (DDL, MERGE, and the Snowpipe writer's row payload
    // keys) agrees on the same bare identifier without needing to quote column references.
    // An unquoted Snowflake identifier can't start with a digit, so a leading digit after
    // sanitizing is prefixed with an underscore.
    public static string SanitizeColumnName(string fieldName)
    {
        var sanitized = NonAlphaNumericRegex.Replace(fieldName, "_").ToUpperInvariant();
        return sanitized.Length > 0 && char.IsDigit(sanitized[0]) ? $"_{sanitized}" : sanitized;
    }

    // SanitizeColumnName is not injective - distinct field names (e.g. "user.email" and
    // "user-email") can sanitize to the same column, which would otherwise produce
    // duplicate columns in the generated DDL and silently drop one value in the writer's
    // row dictionary. Call this before creating/growing a table from a field list, and
    // surface the collision clearly instead of letting either of those happen silently.
    public static void EnsureNoColumnNameCollisions(IReadOnlyList<string> fieldNames)
    {
        var collisions = fieldNames
            .GroupBy(SanitizeColumnName)
            .Where(group => group.Distinct().Count() > 1)
            .ToList();

        if (collisions.Count == 0)
        {
            return;
        }

        var description = string.Join("; ", collisions.Select(group => $"{group.Key} <- [{string.Join(", ", group.Distinct())}]"));
        throw new InvalidOperationException($"Snowflake column name collision: multiple distinct field names sanitize to the same column name ({description}).");
    }

    public static string CreateTransientTableIfNotExists(string database, string schema, string transientTableName, IReadOnlyList<string> fieldNames)
    {
        var qualified = QualifiedName(database, schema, transientTableName);
        var columnDefinitions = string.Join(",\n    ", fieldNames.Select(GetColumnDefinition));
        return $"""
            CREATE TRANSIENT TABLE IF NOT EXISTS {qualified} (
                {columnDefinitions}
            )
            """;
    }

    // The target table is a regular (non-transient) table, since it holds the actual
    // exported data rather than a per-run landing area - transient tables have no
    // fail-safe and a shorter default time-travel retention.
    //
    // The COMMENT clause only takes effect when this statement actually creates the table -
    // IF NOT EXISTS makes the whole statement (including COMMENT) a no-op when the table
    // already existed - so it doubles as an ownership marker: a table with this exact
    // comment was created by this connector, and only such a table is safe for
    // SnowflakeConnector.ArchiveContainer to drop.
    public static string CreateTargetTableIfNotExists(string database, string schema, string targetTableName, IReadOnlyList<string> fieldNames)
    {
        var qualified = QualifiedName(database, schema, targetTableName);
        var columnDefinitions = string.Join(",\n    ", fieldNames.Select(GetColumnDefinition));
        return $"""
            CREATE TABLE IF NOT EXISTS {qualified} (
                {columnDefinitions}
            )
            COMMENT = '{OwnedTableComment.Replace("'", "''")}'
            """;
    }

    // The property set can grow over time (new vocabulary keys), and a table may already
    // exist from before those properties appeared - so on every run, make sure any
    // newly-seen field also has a column, on both the transient and target tables. Returns
    // one ALTER TABLE statement per column (Snowflake's ADD COLUMN IF NOT EXISTS is
    // documented per-column; issuing them separately avoids relying on undocumented
    // multi-column IF NOT EXISTS behavior).
    public static IEnumerable<string> GetAddMissingColumnsStatements(string database, string schema, string tableName, IReadOnlyList<string> fieldNames)
    {
        var qualified = QualifiedName(database, schema, tableName);
        return fieldNames.Select(fieldName => $"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS {GetColumnDefinition(fieldName)}");
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

    public static string DropTableIfExists(string database, string schema, string tableName)
    {
        return $"DROP TABLE IF EXISTS {QualifiedName(database, schema, tableName)}";
    }

    public static string DropPipeIfExists(string database, string schema, string pipeName)
    {
        return $"DROP PIPE IF EXISTS {QualifiedName(database, schema, pipeName)}";
    }

    // Used to check whether the configured role can see a given warehouse/database/schema,
    // for connection verification (see SnowflakeConnector.VerifyDataLakeConnection). A row
    // comes back only for objects that exist and the role has at least one privilege on;
    // this was chosen over "USE WAREHOUSE"/"USE DATABASE"/"USE SCHEMA" (not supported by the
    // SQL API) and over sending the object in a statement's session context (a plain
    // "SELECT 1" doesn't actually resolve database/schema/warehouse, so an invalid one goes
    // unnoticed) - verified live against a real account. The warehouse check specifically is
    // still throttled for health checks (see SnowflakeConnector.HealthCheckWarehouseCheckWindowMinutes) -
    // referencing a warehouse object can resume it, and resuming/keeping it active on every
    // health check tick is a real cost concern regardless of what the query itself needs.
    public static string ShowWarehouses(string warehouseName)
    {
        return $"SHOW WAREHOUSES LIKE '{EscapeLikePattern(warehouseName)}'";
    }

    public static string ShowDatabases(string databaseName)
    {
        return $"SHOW DATABASES LIKE '{EscapeLikePattern(databaseName)}'";
    }

    public static string ShowSchemasInDatabase(string database, string schemaName)
    {
        return $"SHOW SCHEMAS LIKE '{EscapeLikePattern(schemaName)}' IN DATABASE \"{EscapeIdentifier(database)}\"";
    }

    // Used by SnowflakeConnector.ArchiveContainer to read a target table's "comment" column
    // back (see OwnedTableComment/CreateTargetTableIfNotExists) before deciding whether it's
    // safe to drop.
    public static string ShowTablesLikeInSchema(string database, string schema, string tableName)
    {
        return $"SHOW TABLES LIKE '{EscapeLikePattern(tableName)}' IN SCHEMA \"{EscapeIdentifier(database)}\".\"{EscapeIdentifier(schema)}\"";
    }

    // Escaping only the single quote that terminates the string literal isn't enough to
    // make this an exact-name check: '_' and '%' are LIKE wildcards, so e.g. checking for
    // missing warehouse "COMPUTE_WH" could incorrectly succeed against an unrelated,
    // accessible "COMPUTEXWH". Snowflake's default LIKE escape character is a backslash,
    // so every backslash is escaped first (so it isn't itself misread as starting an
    // escape sequence), then '_'/'%' are escaped to match literally.
    private static string EscapeLikePattern(string value)
    {
        return value
            .Replace("\\", "\\\\")
            .Replace("_", "\\_")
            .Replace("%", "\\%")
            .Replace("'", "''");
    }

    // Assumes the target table already exists with the same columns as the transient table
    // minus __ChangeType__ (one per CluedIn property, see the type header above) - see
    // CreateTargetTableIfNotExists/GetAddMissingColumnsStatements, which
    // SnowflakeExportEntitiesJob runs against the target table (with __ChangeType__
    // excluded from fieldNames) before this MERGE.
    public static string MergeTransientIntoTarget(string database, string schema, string transientTableName, string targetTableName, IReadOnlyList<string> fieldNames)
    {
        var qualifiedTarget = QualifiedName(database, schema, targetTableName);
        var qualifiedTransient = QualifiedName(database, schema, transientTableName);

        var idColumn = SanitizeColumnName(StorageConfigurationConstants.IdKey);
        var changeTypeColumn = SanitizeColumnName(StorageConfigurationConstants.ChangeTypeKey);
        var persistVersionColumn = SanitizeColumnName(StorageConfigurationConstants.PersistVersionKey);

        // __ChangeType__ is transient-only bookkeeping used below to decide which MERGE
        // action to take - it's never a real CluedIn property, so it's read from the
        // transient table (sourceColumns, for the WHEN clauses) but excluded from what
        // actually gets INSERTed/UPDATEd into the target table (targetColumns).
        var sourceColumns = fieldNames.Select(SanitizeColumnName).ToList();
        var sourceColumnList = string.Join(", ", sourceColumns);

        var targetColumns = sourceColumns.Where(column => column != changeTypeColumn).ToList();
        var targetColumnList = string.Join(", ", targetColumns);
        var updateList = string.Join(", ", targetColumns
            .Where(column => column != idColumn)
            .Select(column => $"target.{column} = source.{column}"));
        var insertValueList = string.Join(", ", targetColumns.Select(column => $"source.{column}"));

        return $"""
            MERGE INTO {qualifiedTarget} AS target
            USING (
                SELECT {sourceColumnList}
                FROM (
                    SELECT {sourceColumnList},
                        ROW_NUMBER() OVER (PARTITION BY {idColumn} ORDER BY {persistVersionColumn} DESC) AS RN
                    FROM {qualifiedTransient}
                )
                WHERE RN = 1
            ) AS source
            ON target.{idColumn} = source.{idColumn}
            WHEN MATCHED AND source.{changeTypeColumn} = 'Removed' THEN DELETE
            WHEN MATCHED THEN UPDATE SET {updateList}
            WHEN NOT MATCHED AND source.{changeTypeColumn} != 'Removed' THEN INSERT ({targetColumnList}) VALUES ({insertValueList})
            """;
    }

    private static string GetColumnDefinition(string fieldName)
    {
        var columnType = fieldName == StorageConfigurationConstants.PersistVersionKey ? "NUMBER" : "VARCHAR";
        return $"{SanitizeColumnName(fieldName)} {columnType}";
    }
}
