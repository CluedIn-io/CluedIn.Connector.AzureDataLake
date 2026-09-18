#if CLUEDIN_V50_OR_GREATER
using System;

using Castle.MicroKernel.Registration;
using Castle.Windsor;

using CluedIn.Core;

// We are defining the IDateTimeOffsetProvider interface and its implementation in the CluedIn.Core namespace
// CluedIn.Core removed the IDateTimeOffsetProvider interface and its implementation in v5.0, but we still need it for our connector to work
// This is a temporary solution to keep our connector working until we migrate to the new TimeProvider interface in CluedIn.Core
namespace CluedIn.Core
{
    public interface IDateTimeOffsetProvider
    {
        DateTimeOffset GetCurrentUtcTime();
        DateTimeOffset GetCurrentTime();
    }

    internal class DateTimeOffsetProvider : IDateTimeOffsetProvider
    {
        public DateTimeOffset GetCurrentUtcTime() => DateTimeOffset.UtcNow;

        public DateTimeOffset GetCurrentTime() => DateTimeOffset.Now;
    }
}

namespace CluedIn.Connector.FileStorage.Common
{

    internal abstract partial class InstallComponentsBase : IWindsorInstaller
    {
        private static partial void RegisterDateTimeProvider(IWindsorContainer container)
        {
            container.Register(Component.For<IDateTimeOffsetProvider>().ImplementedBy<DateTimeOffsetProvider>().LifestyleSingleton());
        }
    }
}

#endif
