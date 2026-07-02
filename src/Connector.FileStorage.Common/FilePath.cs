namespace CluedIn.Connector.FileStorage.Common;

public record FilePath(string Name, DirectoryPath DirectoryPath)
{
    public string FullPath => $"{DirectoryPath.Path}/{Name}";
}

public record FullyQualifiedFilePath(string Name, DirectoryPath DirectoryPath, string FullyQualifiedName) : FilePath(Name, DirectoryPath);
