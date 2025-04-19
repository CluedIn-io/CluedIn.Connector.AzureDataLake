using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Fabric.Api.MirroredDatabase.Models;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

internal interface IOpenMirrorringDataLakeClient : IDataLakeClient
{
    Task<MirroredDatabase> UpdateOrCreateMirroredDatabaseAsync(IDataLakeJobData dataLakeJobData, bool isEnabled);
}
