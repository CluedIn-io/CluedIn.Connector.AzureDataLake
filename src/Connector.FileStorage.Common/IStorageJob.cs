using System.Threading.Tasks;

using CluedIn.Core;

namespace CluedIn.Connector.FileStorage.Common;

public interface IStorageJob
{
    Task<bool> CanRunAsync(ExecutionContext context, IStorageJobArgs args);
    Task DoRunAsync(ExecutionContext context, IStorageJobArgs args);
}
