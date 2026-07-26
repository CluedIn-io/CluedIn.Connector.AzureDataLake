using CluedIn.Core.Connectors;

namespace CluedIn.Connector.FileStorage.Common.Connector;

public class FileStorageConnectionVerificationResult(
    bool success,
    string errorMessage = null,
    bool hasException = false)
    : ConnectionVerificationResult(success, errorMessage)
{
    public bool HasException { get; } = hasException;
}
