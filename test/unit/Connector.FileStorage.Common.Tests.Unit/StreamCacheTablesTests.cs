using System;

using CluedIn.Connector.FileStorage.Common.Connector;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Unit;

public class StreamCacheTablesTests
{
    private static readonly Guid _streamId = Guid.NewGuid();
    private static readonly string _table = $"Stream_{_streamId}";

    [Fact]
    public void ObsoleteTableNames_AreTheArchivedTablesExceptTheMostRecent()
    {
        var tables = Assert.Single(StreamCacheTables.FromTableNames(
            new[] { $"{_table}_20250102000000", _table, $"{_table}_20260101000000", $"{_table}_20250101000000" }));

        Assert.Equal(_streamId, tables.StreamId);
        Assert.Equal(4, tables.TableNames.Count);
        Assert.Equal(new[] { $"{_table}_20250102000000", $"{_table}_20250101000000" }, tables.ObsoleteTableNames);
    }

    [Fact]
    public void ObsoleteTableNames_WithASingleArchivedTable_IsEmpty()
    {
        var tables = Assert.Single(StreamCacheTables.FromTableNames(new[] { _table, $"{_table}_20260101000000" }));

        Assert.Empty(tables.ObsoleteTableNames);
    }

    [Theory]
    [InlineData("_2026010100000")]
    [InlineData("_202601010000000")]
    [InlineData("_20261301000000")]
    [InlineData("_backup")]
    [InlineData("_20260101000000_backup")]
    public void ArchivedTableNames_DoNotContainATableWithAnotherSuffix(string suffix)
    {
        var tables = Assert.Single(StreamCacheTables.FromTableNames(new[] { _table + suffix }));

        Assert.Empty(tables.ArchivedTableNames);
    }

    [Fact]
    public void FromTableNames_GroupsByStream()
    {
        var tables = StreamCacheTables.FromTableNames(
            new[] { $"{_table}_20250101000000", $"Stream_{Guid.NewGuid()}_20260101000000" });

        Assert.Equal(2, tables.Count);
        Assert.All(tables, streamTables => Assert.Empty(streamTables.ObsoleteTableNames));
    }

    [Theory]
    [InlineData("Other")]
    [InlineData("Stream_")]
    [InlineData("Stream_notaguid_20260101000000")]
    [InlineData("testConnection_7a6a2b8e-6f0c-4f0d-9d8c-2a7c8a1f5b11_20260101000000")]
    [InlineData("Stream_7a6a2b8e-6f0c-4f0d-9d8c-2a7c8a1f5b11x")]
    public void FromTableNames_IgnoresOtherTables(string tableName)
    {
        Assert.Empty(StreamCacheTables.FromTableNames(new[] { tableName }));
    }
}
