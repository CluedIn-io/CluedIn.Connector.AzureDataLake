#if !CLUEDIN_V50_OR_GREATER
using System;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

namespace CluedIn.Connector.FileStorage.Common;

internal abstract partial class InstallComponentsBase : IWindsorInstaller
{
    private static partial void RegisterDateTimeProvider(IWindsorContainer container)
    {
        // No implementation for versions prior to v5.0
    }
}
#endif
