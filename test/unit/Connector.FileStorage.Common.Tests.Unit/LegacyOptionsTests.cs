using System.Collections.Generic;
using System.Linq;

using CluedIn.Core;
using CluedIn.Core.Providers;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Unit;

public class LegacyOptionsTests
{
    private sealed class TestConstants : StorageConfigurationConstants
    {
        private TestConstants(ExportOptionsSettings settings)
            : base(
                System.Guid.NewGuid(),
                "Test",
                "Test",
                "icon",
                "domain",
                "about",
                new AuthMethods(),
                "details",
                properties: GetExportOptions(settings))
        {
        }

        protected override string CacheKeyword => "Test";

        public static List<Control> ExportEverything() => GetExportOptions(Everything);

        public static List<Control> ExportWithForcedStreamCache() => GetExportOptions(new(IsForceStreamCache: true));

        private static ExportOptionsSettings Everything { get; } = new(
            IsArrayColumnOptionEnabled: true,
            IsDeltaOptionEnabled: true,
            IsEscapeVocabularyKeysOptionEnabled: true,
            IsWriteGuidAsStringOptionEnabled: true);

        public static TestConstants CreateWithEverything() => new(Everything);
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData("true", true)]
    [InlineData("false", false)]
    [InlineData("garbage", true)]
    public void IsUsingLegacyOptions_DefaultsToTrueUnlessExplicitlyFalse(object value, bool expected)
    {
        var configurations = new Dictionary<string, object>();
        if (value != null)
        {
            configurations[StorageConfigurationConstants.UseLegacyOptions] = value;
        }

        Assert.Equal(expected, StorageConfigurationConstants.IsUsingLegacyOptions(configurations));
    }

    [Fact]
    public void IsUsingLegacyOptions_NullValue_IsTrue()
    {
        var configurations = new Dictionary<string, object> { [StorageConfigurationConstants.UseLegacyOptions] = null };

        Assert.True(StorageConfigurationConstants.IsUsingLegacyOptions(configurations));
    }

    [Fact]
    public void ExportOptions_AreAllKeysThatCanBeMovedToStream()
    {
        var names = TestConstants.ExportEverything().Select(control => control.Name).ToHashSet();

        Assert.Equal(StorageConfigurationConstants.ExportOptionKeys.ToHashSet(), names);
    }

    [Fact]
    public void ExportOptions_ReturnNewInstancesOnEveryCall()
    {
        var first = TestConstants.ExportEverything();
        var second = TestConstants.ExportEverything();

        Assert.All(first, control => Assert.DoesNotContain(control, second));
    }

    [Fact]
    public void Constants_PassExportOptionsAsProperties()
    {
        var constants = TestConstants.CreateWithEverything();

        Assert.Equal(
            StorageConfigurationConstants.ExportOptionKeys.ToHashSet(),
            constants.Properties.Select(control => control.Name).ToHashSet());
    }

    [Fact]
    public void ForcedStreamCache_DoesNotOfferStreamCacheSwitch()
    {
        var names = TestConstants.ExportWithForcedStreamCache().Select(control => control.Name);

        Assert.DoesNotContain(StorageConfigurationConstants.IsStreamCacheEnabled, names);
    }

    [Fact]
    public void Merge_LegacyOptions_IgnoresStreamExportOptionsButAppliesOtherProperties()
    {
        var authentication = new Dictionary<string, object>
        {
            [StorageConfigurationConstants.OutputFormat] = "CSV",
            ["AccountName"] = "connector",
        };
        var stream = new Dictionary<string, object>
        {
            [StorageConfigurationConstants.OutputFormat] = "JSON",
            ["AccountName"] = "stream",
        };

        StorageFactoryBase.MergeConnectorProperties(authentication, stream, StorageConfigurationConstants.ExportOptionKeys);

        Assert.Equal("CSV", authentication[StorageConfigurationConstants.OutputFormat]);
        Assert.Equal("stream", authentication["AccountName"]);
    }

    [Fact]
    public void Merge_LegacyOptionsDefaultedFromMissingKey_KeepsConnectorExportOptions()
    {
        var authentication = new Dictionary<string, object> { [StorageConfigurationConstants.Schedule] = "Daily" };

        StorageFactoryBase.MergeConnectorProperties(
            authentication,
            new Dictionary<string, object> { [StorageConfigurationConstants.Schedule] = "Hourly" },
            StorageConfigurationConstants.ExportOptionKeys);

        Assert.Equal("Daily", authentication[StorageConfigurationConstants.Schedule]);
    }

    [Fact]
    public void Merge_NonLegacyOptions_UsesStreamExportOptionsAndDropsConnectorOnes()
    {
        var authentication = new Dictionary<string, object>
        {
            [StorageConfigurationConstants.UseLegacyOptions] = false,
            [StorageConfigurationConstants.OutputFormat] = "CSV",
            [StorageConfigurationConstants.IsDeltaMode] = true,
            [StorageConfigurationConstants.StreamCacheConnectionString] = "connection",
        };
        var stream = new Dictionary<string, object> { [StorageConfigurationConstants.OutputFormat] = "Parquet" };

        StorageFactoryBase.MergeConnectorProperties(authentication, stream, StorageConfigurationConstants.ExportOptionKeys);

        Assert.Equal("Parquet", authentication[StorageConfigurationConstants.OutputFormat]);
        Assert.DoesNotContain(StorageConfigurationConstants.IsDeltaMode, authentication.Keys);
        Assert.Equal("connection", authentication[StorageConfigurationConstants.StreamCacheConnectionString]);
    }

    [Fact]
    public void Merge_NonLegacyOptionsAndNoStreamProperties_LeavesNoExportOptions()
    {
        var authentication = new Dictionary<string, object>
        {
            [StorageConfigurationConstants.UseLegacyOptions] = false,
            [StorageConfigurationConstants.Schedule] = "Daily",
        };

        StorageFactoryBase.MergeConnectorProperties(authentication, null, StorageConfigurationConstants.ExportOptionKeys);

        Assert.DoesNotContain(StorageConfigurationConstants.Schedule, authentication.Keys);
    }
}
