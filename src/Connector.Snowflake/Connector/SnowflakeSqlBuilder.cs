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

    public static string QualifiedName(string database, string schema, string objectName)
    {
        return $"\"{database}\".\"{schema}\".\"{objectName}\"";
    }

    // Snowflake identifiers fold to upper-case unless quoted; sanitizing and upper-casing
    // here up front means every caller (DDL, MERGE, and the Snowpipe writer's row payload
    // keys) agrees on the same bare identifier without needing to quote column references.
    public static string SanitizeColumnName(string fieldName)
    {
        return NonAlphaNumericRegex.Replace(fieldName, "_").ToUpperInvariant();
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
    public static string CreateTargetTableIfNotExists(string database, string schema, string targetTableName, IReadOnlyList<string> fieldNames)
    {
        var qualified = QualifiedName(database, schema, targetTableName);
        var columnDefinitions = string.Join(",\n    ", fieldNames.Select(GetColumnDefinition));
        return $"""
            CREATE TABLE IF NOT EXISTS {qualified} (
                {columnDefinitions}
            )
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

    // Assumes the target table already exists with the same columns as the transient table
    // (one per CluedIn property, see the type header above) - see
    // CreateTargetTableIfNotExists/GetAddMissingColumnsStatements, which
    // SnowflakeExportEntitiesJob runs against the target table before this MERGE.
    public static string MergeTransientIntoTarget(string database, string schema, string transientTableName, string targetTableName, IReadOnlyList<string> fieldNames)
    {
        var qualifiedTarget = QualifiedName(database, schema, targetTableName);
        var qualifiedTransient = QualifiedName(database, schema, transientTableName);

        var idColumn = SanitizeColumnName(StorageConfigurationConstants.IdKey);
        var changeTypeColumn = SanitizeColumnName(StorageConfigurationConstants.ChangeTypeKey);
        var persistVersionColumn = SanitizeColumnName(StorageConfigurationConstants.PersistVersionKey);

        var columns = fieldNames.Select(SanitizeColumnName).ToList();
        var columnList = string.Join(", ", columns);
        var updateList = string.Join(", ", columns
            .Where(column => column != idColumn)
            .Select(column => $"target.{column} = source.{column}"));
        var insertValueList = string.Join(", ", columns.Select(column => $"source.{column}"));

        return $"""
            MERGE INTO {qualifiedTarget} AS target
            USING (
                SELECT {columnList}
                FROM (
                    SELECT {columnList},
                        ROW_NUMBER() OVER (PARTITION BY {idColumn} ORDER BY {persistVersionColumn} DESC) AS RN
                    FROM {qualifiedTransient}
                )
                WHERE RN = 1
            ) AS source
            ON target.{idColumn} = source.{idColumn}
            WHEN MATCHED AND source.{changeTypeColumn} = 'Removed' THEN DELETE
            WHEN MATCHED THEN UPDATE SET {updateList}
            WHEN NOT MATCHED AND source.{changeTypeColumn} != 'Removed' THEN INSERT ({columnList}) VALUES ({insertValueList})
            """;
    }

    private static string GetColumnDefinition(string fieldName)
    {
        var columnType = fieldName == StorageConfigurationConstants.PersistVersionKey ? "NUMBER" : "VARCHAR";
        return $"{SanitizeColumnName(fieldName)} {columnType}";
    }
}
