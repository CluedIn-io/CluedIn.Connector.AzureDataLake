using System.Threading.Tasks;

using CluedIn.Core;

namespace CluedIn.Connector.FileStorage.Common;

public interface IDataLakeJob
{
    Task DoRunAsync(ExecutionContext context, IDataLakeJobArgs args);
}
