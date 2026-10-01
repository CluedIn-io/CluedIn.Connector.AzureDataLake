using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Integration;

// Assert.Skip is an xunit v3-only API (dynamic skip, recognized by the v3 runner via a
// thrown SkipException) - xunit v2 (used for CluedIn <5.0.0, net6.0 - see
// Directory.Build.props/test/Directory.Build.props) has no equivalent. Every call site
// pairs this with its own `return;` immediately after, so on v2 the test simply passes
// trivially instead of reporting Skipped - there's no way to get a real "Skipped" result
// without pulling in an extra package (e.g. Xunit.SkippableFact) that isn't referenced
// anywhere else in this repo.
internal static class DynamicSkip
{
    public static void Request(string reason)
    {
#if CLUEDIN_V50_OR_GREATER
        Assert.Skip(reason);
#endif
    }
}
