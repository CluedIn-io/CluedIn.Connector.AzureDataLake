using System;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Unit;

public class ConfigurationHelperTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void GetValue_WithoutAKeyName_ReturnsTheDefaultValue(string keyName)
    {
        // A mock of the configuration constants returns null for a key name it was not set up with
        Assert.Equal(3, ConfigurationHelper.GetValue(keyName, 3));
        Assert.Equal("default", ConfigurationHelper.GetValue(keyName, "default"));
        Assert.True(ConfigurationHelper.GetValue(keyName, true));
    }

    [Fact]
    public void GetValue_ForASettingThatIsNotSet_ReturnsTheDefaultValue()
    {
        var keyName = $"Streams.Tests.NotSet.{Guid.NewGuid():N}";

        Assert.Equal(42, ConfigurationHelper.GetValue(keyName, 42));
    }
}
