using System;

namespace CluedIn.Connector.FileStorage.Common;

public record DirectoryPath(string Path)
{
    internal string Path { get; init; } = Path.TrimEnd('/');

    public FilePath GetFilePath(string fileName) => new (fileName, this);

    public DirectoryPath GetSubDirectoryPath(string directoryName)
    {
        if (string.IsNullOrWhiteSpace(directoryName))
        {
            throw new ArgumentException($"{nameof(directoryName)} cannot be null or empty.");
        }

        return new DirectoryPath($"{Path}/{directoryName}"); //TODO: Path combine?
    }
}
