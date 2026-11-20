using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Integration;

// Sets Skip in the constructor (evaluated once at test discovery) rather than returning
// early from the test body like DynamicSkip.Request does - this reports a real "Skipped"
// result in both xunit v2 and v3, instead of only on v3 (see DynamicSkip.cs), and needs no
// extra package (e.g. Xunit.SkippableFact).
internal sealed class SnowflakeFactAttribute : FactAttribute
{
    public SnowflakeFactAttribute()
    {
        if (!SnowflakeTestCredentials.IsAvailable)
        {
            Skip = SnowflakeTestCredentials.SkipReason;
        }
    }
}
