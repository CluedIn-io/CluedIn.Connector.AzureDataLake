using System.Collections.Generic;

using CluedIn.Connector.Snowflake.Connector.Snowpipe;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Unit;

public class SnowflakeStatementResultExtensionsTests
{
    private static SnowflakeStatementResult CreateResult(params string[] names)
    {
        var columnNames = new List<string> { "created_on", "name", "comment" };
        var rows = new List<IReadOnlyList<string>>();
        foreach (var name in names)
        {
            rows.Add(new List<string> { "2024-01-01", name, "" });
        }

        return new SnowflakeStatementResult(true, columnNames, rows);
    }

    [Fact]
    public void HasExactNameMatch_RowWithExactName_ReturnsTrue()
    {
        var result = CreateResult("MYTABLE");

        Assert.True(result.HasExactNameMatch("MYTABLE"));
    }

    [Fact]
    public void HasExactNameMatch_IsCaseInsensitive()
    {
        var result = CreateResult("MYTABLE");

        Assert.True(result.HasExactNameMatch("mytable"));
    }

    // The whole reason this helper exists: SHOW ... LIKE can return a row that merely
    // matches the wildcard pattern (e.g. LIKE 'MYTABLE%' also matching 'MYTABLE_ARCHIVED'),
    // not one that's an exact match for the name the caller asked about.
    [Fact]
    public void HasExactNameMatch_OnlyLikeMatchingRowPresent_ReturnsFalse()
    {
        var result = CreateResult("MYTABLE_ARCHIVED_20240101120000");

        Assert.False(result.HasExactNameMatch("MYTABLE"));
    }

    [Fact]
    public void HasExactNameMatch_NoRows_ReturnsFalse()
    {
        var result = CreateResult();

        Assert.False(result.HasExactNameMatch("MYTABLE"));
    }

    [Fact]
    public void HasExactNameMatch_NoNameColumn_ReturnsFalse()
    {
        var result = new SnowflakeStatementResult(
            true,
            new List<string> { "created_on", "comment" },
            new List<IReadOnlyList<string>> { new List<string> { "2024-01-01", "" } });

        Assert.False(result.HasExactNameMatch("MYTABLE"));
    }

    [Fact]
    public void TryGetExactNameMatch_ExactAndLikeMatchingRowsPresent_ReturnsOnlyExactRow()
    {
        var result = CreateResult("MYTABLE_ARCHIVED_20240101120000", "MYTABLE");

        var found = result.TryGetExactNameMatch("MYTABLE", out var row);

        Assert.True(found);
        Assert.Equal("MYTABLE", row[1]);
    }

    [Fact]
    public void TryGetExactNameMatch_NotFound_RowIsNull()
    {
        var result = CreateResult("OTHERTABLE");

        var found = result.TryGetExactNameMatch("MYTABLE", out var row);

        Assert.False(found);
        Assert.Null(row);
    }
}
