using System;

namespace CluedIn.Connector.FileStorage.Common;

public record DirectoryPath(string Path)
{
    public FilePath GetFilePath(string fileName) => new FilePath(fileName, this);
    public DirectoryPath GetSubDirectoryPath(string directoryName)
    {
        if (string.IsNullOrWhiteSpace(directoryName))
        {
            throw new ArgumentException($"{nameof(directoryName)} cannot be null or empty.");
        }

        return new DirectoryPath(this.Path + "/" + directoryName); //TODO: Path combine?
    }
}
