using System;
using System.Data;

using CluedIn.Connector.FileStorage.Common.Connector;

using Microsoft.Data.SqlClient;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Unit;

public class CacheTableParameterHelperTests
{
    public static TheoryData<Type, object> ColumnTypesAndValues => new()
    {
        { typeof(string), "value" },
        { typeof(bool), true },
        { typeof(int), 42 },
        { typeof(long), 42L },
        { typeof(double), 1.5d },
        { typeof(decimal), 123.45m },
        { typeof(DateTime), new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc) },
        { typeof(DateTimeOffset), new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero) },
        { typeof(Guid), Guid.NewGuid() },
    };

    [Theory]
    [InlineData("", "x")]
    [InlineData("a", "a much longer string value")]
    public void CreateParameter_StringsOfDifferentLength_HaveSameDeclaration(string first, string second)
    {
        // act
        var firstParameter = CacheTableParameterHelper.CreateParameter("@p0", first, typeof(string));
        var secondParameter = CacheTableParameterHelper.CreateParameter("@p0", second, typeof(string));

        // assert
        Assert.Equal(SqlDbType.NVarChar, firstParameter.SqlDbType);
        Assert.Equal(-1, firstParameter.Size);
        AssertSameDeclaration(firstParameter, secondParameter);
    }

    [Fact]
    public void CreateParameter_StringOverMaxNonLobLength_HasSameDeclarationAsShortString()
    {
        // act
        var shortParameter = CacheTableParameterHelper.CreateParameter("@p0", "short", typeof(string));
        var longParameter = CacheTableParameterHelper.CreateParameter("@p0", new string('x', 5000), typeof(string));

        // assert
        AssertSameDeclaration(shortParameter, longParameter);
    }

    [Fact]
    public void CreateParameter_DecimalsWithDifferentPrecisionAndScale_HaveSameDeclaration()
    {
        // act
        var firstParameter = CacheTableParameterHelper.CreateParameter("@p0", 1.5m, typeof(decimal));
        var secondParameter = CacheTableParameterHelper.CreateParameter("@p0", 123456789.123456m, typeof(decimal));

        // assert
        Assert.Equal(SqlDbType.Decimal, firstParameter.SqlDbType);
        Assert.Equal(CacheTableParameterHelper.DecimalPrecision, firstParameter.Precision);
        Assert.Equal(CacheTableParameterHelper.DecimalScale, firstParameter.Scale);
        AssertSameDeclaration(firstParameter, secondParameter);
    }

    [Theory]
    [MemberData(nameof(ColumnTypesAndValues))]
    public void CreateParameter_NullValue_HasSameDeclarationAsNonNullValue(Type columnType, object value)
    {
        // act
        var withValue = CacheTableParameterHelper.CreateParameter("@p0", value, columnType);
        var withNull = CacheTableParameterHelper.CreateParameter("@p0", DBNull.Value, columnType);

        // assert
        AssertSameDeclaration(withValue, withNull);
        Assert.Equal(DBNull.Value, withNull.Value);
    }

    [Fact]
    public void CreateParameter_DateTime_KeepsDateTimeDeclaration()
    {
        // act
        var parameter = CacheTableParameterHelper.CreateParameter("@p0", DateTime.UtcNow, typeof(DateTime));

        // assert
        Assert.Equal(SqlDbType.DateTime, parameter.SqlDbType);
    }

    [Fact]
    public void CreateParameter_ValueTypeDifferentFromColumnType_KeepsValueType()
    {
        // act
        var parameter = CacheTableParameterHelper.CreateParameter("@p0", 42, typeof(string));

        // assert
        Assert.Equal(SqlDbType.Int, parameter.SqlDbType);
        Assert.Equal(42, parameter.Value);
    }

    [Fact]
    public void CreateParameter_UnmappedColumnTypeWithNullValue_IsDeclaredAsNVarCharMax()
    {
        // act
        var parameter = CacheTableParameterHelper.CreateParameter("@p0", DBNull.Value, typeof(string[]));

        // assert
        Assert.Equal(SqlDbType.NVarChar, parameter.SqlDbType);
        Assert.Equal(-1, parameter.Size);
    }

    private static void AssertSameDeclaration(SqlParameter expected, SqlParameter actual)
    {
        Assert.Equal(expected.SqlDbType, actual.SqlDbType);
        Assert.Equal(expected.Size, actual.Size);
        Assert.Equal(expected.Precision, actual.Precision);
        Assert.Equal(expected.Scale, actual.Scale);
    }
}
