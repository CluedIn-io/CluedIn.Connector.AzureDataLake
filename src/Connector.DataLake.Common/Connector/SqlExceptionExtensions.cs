using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal static class SqlExceptionExtensions
{
    public static bool IsTableNotFoundException(this SqlException ex)
    {
        return ex?.Number == 208;
    }
}
