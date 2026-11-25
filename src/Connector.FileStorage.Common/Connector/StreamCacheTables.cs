using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CluedIn.Connector.FileStorage.Common.Connector;

/// <summary>
/// The stream cache tables of a stream, without the history tables that belong to them.
/// </summary>
/// <param name="StreamId">The stream.</param>
/// <param name="TableNames">All the tables of the stream.</param>
/// <param name="ArchivedTableNames">Those with a date suffix, which were renamed when the stream was archived, most recent first.</param>
internal sealed record StreamCacheTables(Guid StreamId, IReadOnlyList<string> TableNames, IReadOnlyList<string> ArchivedTableNames)
{
    internal const string ArchiveDateFormat = "yyyyMMddHHmmss";

    private const string TablePrefix = "Stream_";
    private const int GuidLength = 36;

    /// <summary>
    /// The archived tables except for the most recent one.
    /// </summary>
    public IReadOnlyList<string> ObsoleteTableNames => ArchivedTableNames.Skip(1).ToList();

    /// <summary>
    /// Groups table names by stream. Names that are not those of a stream cache table are ignored.
    /// </summary>
    public static IReadOnlyList<StreamCacheTables> FromTableNames(IEnumerable<string> tableNames)
    {
        return tableNames
            .Select(tableName => (TableName: tableName, IsStreamTable: TryParse(tableName, out var streamId, out var suffix), StreamId: streamId, Suffix: suffix))
            .Where(table => table.IsStreamTable)
            .GroupBy(table => table.StreamId)
            .Select(group => new StreamCacheTables(
                group.Key,
                group.Select(table => table.TableName).ToList(),
                group
                    .Where(table => IsArchiveSuffix(table.Suffix))
                    .OrderByDescending(table => table.Suffix, StringComparer.Ordinal)
                    .Select(table => table.TableName)
                    .ToList()))
            .ToList();
    }

    private static bool TryParse(string tableName, out Guid streamId, out string suffix)
    {
        streamId = Guid.Empty;
        suffix = string.Empty;
        var nameLength = TablePrefix.Length + GuidLength;
        if (tableName.Length < nameLength
            || !tableName.StartsWith(TablePrefix, StringComparison.OrdinalIgnoreCase)
            || !Guid.TryParseExact(tableName.Substring(TablePrefix.Length, GuidLength), "D", out streamId))
        {
            return false;
        }

        suffix = tableName[nameLength..];
        return suffix.Length == 0 || suffix[0] == '_';
    }

    private static bool IsArchiveSuffix(string suffix)
    {
        return suffix.Length == ArchiveDateFormat.Length + 1
            && DateTime.TryParseExact(suffix[1..], ArchiveDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out _);
    }
}
