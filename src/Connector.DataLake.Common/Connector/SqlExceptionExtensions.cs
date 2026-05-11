using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal static class SqlExceptionExtensions
{
    public static bool IsTableNotFoundException(this SqlException ex)
    {
        return ex?.Number == 208;
    }

    public static bool IsCannotFindTableException(this SqlException ex)
    {
        return ex?.Number == 4902;
    }

    public static bool IsColumnAlreadyExistsException(this SqlException ex)
    {
        return ex?.Number == 2705;
    }
}
