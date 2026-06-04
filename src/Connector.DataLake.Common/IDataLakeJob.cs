using System.Threading.Tasks;

using CluedIn.Core;

namespace CluedIn.Connector.DataLake.Common;

public interface IDataLakeJob
{
    Task<bool> CanRunAsync(ExecutionContext context, IDataLakeJobArgs args);
    Task DoRunAsync(ExecutionContext context, IDataLakeJobArgs args);
}
