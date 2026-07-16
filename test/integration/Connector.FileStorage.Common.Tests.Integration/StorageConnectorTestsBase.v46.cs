#if !CLUEDIN_V47_OR_GREATER
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Moq;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

public abstract partial class StorageConnectorTestsBase<TConnector, TClientFactory, TConfigurationConstants>
{
    private static partial void SetupGetStream(Mock<IStreamRepository> streamRepository, StreamModel streamModel)
    {
        streamRepository.Setup(x => x.GetStream(streamModel.Id)).ReturnsAsync(streamModel);
    }
}
#endif
