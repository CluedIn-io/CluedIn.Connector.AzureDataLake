using System;
using System.Collections.Generic;
using System.Data;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.FileStorage.Common.Connector;

/// <summary>
/// Creates explicitly typed parameters for cache table commands.
/// </summary>
/// <remarks>
/// When a parameter has no explicit type, SqlClient infers its declaration from the value
/// (e.g. nvarchar(17) for a 17 character string, decimal(5,2) for 123.45, nvarchar for DBNull).
/// The parameter declaration is part of the plan cache key for sp_executesql, so untyped parameters
/// produce a new single-use plan for almost every call. Pinning the declaration lets every call against
/// the same cache table share one plan.
/// </remarks>
internal static class CacheTableParameterHelper
{
    // Wide enough that the client never drops digits that the server-side conversion
    // to the column's DECIMAL type would have used when rounding.
    internal const byte DecimalPrecision = 38;
    internal const byte DecimalScale = 18;

    private static readonly Dictionary<Type, SqlDbType> _dotNetToSqlDbTypeMap = new()
    {
        [typeof(bool)] = SqlDbType.Bit,
        [typeof(byte)] = SqlDbType.TinyInt,
        [typeof(short)] = SqlDbType.SmallInt,
        [typeof(int)] = SqlDbType.Int,
        [typeof(long)] = SqlDbType.BigInt,
        [typeof(float)] = SqlDbType.Real,
        [typeof(double)] = SqlDbType.Float,
        [typeof(decimal)] = SqlDbType.Decimal,
        // Matches what SqlClient infers for DateTime so stored values are unchanged
        [typeof(DateTime)] = SqlDbType.DateTime,
        [typeof(DateTimeOffset)] = SqlDbType.DateTimeOffset,
        [typeof(TimeSpan)] = SqlDbType.Time,
        [typeof(Guid)] = SqlDbType.UniqueIdentifier,
        [typeof(string)] = SqlDbType.NVarChar,
    };

    /// <summary>
    /// Creates a parameter whose declaration depends only on the value's type, never on the value itself.
    /// </summary>
    /// <param name="parameterName">Name of the parameter, including the '@' prefix.</param>
    /// <param name="value">Value to send. A null or <see cref="DBNull"/> value takes its type from <paramref name="columnType"/>.</param>
    /// <param name="columnType">.NET type the cache table column was created from.</param>
    /// <remarks>
    /// Non-null values keep their own type so that the server performs the same implicit conversion
    /// into the column as it did before parameters were typed.
    /// </remarks>
    internal static SqlParameter CreateParameter(string parameterName, object value, Type columnType)
    {
        var valueType = value is null || value is DBNull ? columnType : value.GetType();
        var sqlDbType = _dotNetToSqlDbTypeMap.TryGetValue(valueType, out var mappedType) ? mappedType : SqlDbType.NVarChar;

        var parameter = new SqlParameter(parameterName, sqlDbType)
        {
            Value = value,
        };

        switch (sqlDbType)
        {
            case SqlDbType.NVarChar:
                // String columns are NVARCHAR(MAX)
                parameter.Size = -1;
                break;
            case SqlDbType.Decimal:
                parameter.Precision = DecimalPrecision;
                parameter.Scale = DecimalScale;
                break;
        }

        return parameter;
    }
}
