using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.DataLake.Common.Connector
{
    public interface IExternalStorageClient
    {
        Task EnsureDirectoryExists(object configuration, string subDirectory = null);
        Task SaveData(object configuration, string content, string fileName, string contentType);
        Task DeleteDirectory(object configuration, string subDirectory);
        Task DeleteFile(object configuration, string fileName);
        Task<bool> FileExists(object configuration, string fileName, string subDirectory = null);
        Task<bool> DirectoryExists(object configuration, string subDirectory = null);
        Task<IEnumerable<string>> ListFiles(object configuration, string subDirectory = null);
    }
}
