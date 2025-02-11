using System;
using System.Collections.Generic;

namespace CluedIn.Connector.DataLake.Common;

internal abstract class DataLakeJobData : CrawlJobDataWrapper, IDataLakeJobData
{
    protected DataLakeJobData(IDictionary<string, object> configurations, string containerName = null) : base(configurations)
    {
        ContainerName = containerName;
    }

    public string OutputFormat => GetConfigurationValue(DataLakeConstants.OutputFormat) as string ?? DataLakeConstants.OutputFormats.Json;
    public bool IsStreamCacheEnabled => GetConfigurationValue(DataLakeConstants.IsStreamCacheEnabled) as bool? ?? false;
    public string StreamCacheConnectionString => GetConfigurationValue(DataLakeConstants.StreamCacheConnectionString) as string;
    public string Schedule => GetConfigurationValue(DataLakeConstants.Schedule) as string;
    public string ContainerName { get; }
    public bool UseCurrentTimeForExport => GetConfigurationValue(DataLakeConstants.UseCurrentTimeForExport) as bool? ?? false;
    public string FileNamePattern => GetConfigurationValue(DataLakeConstants.FileNamePattern) as string;
    public virtual bool ShouldWriteGuidAsString => GetConfigurationValue(DataLakeConstants.ShouldWriteGuidAsString) as bool? ?? false;
    public virtual bool ShouldEscapeVocabularyKeys => GetConfigurationValue(DataLakeConstants.ShouldEscapeVocabularyKeys) as bool? ?? false;
    public string CustomCron => GetConfigurationValue(DataLakeConstants.CustomCron) as string;
    public virtual bool IsDeltaMode => GetConfigurationValue(DataLakeConstants.IsDeltaMode) as bool? ?? false;
    public virtual bool IsOverwriteEnabled => GetConfigurationValue(DataLakeConstants.IsOverwriteEnabled) as bool? ?? true;

    public override int GetHashCode()
    {
        var hash = new HashCode();
        AddToHashCode(hash);
        return hash.ToHashCode();
    }

    protected virtual void AddToHashCode(HashCode hash)
    {
        hash.Add(OutputFormat);
        hash.Add(IsStreamCacheEnabled);
        hash.Add(StreamCacheConnectionString);
        hash.Add(Schedule);
        hash.Add(ContainerName);
        hash.Add(UseCurrentTimeForExport);
        hash.Add(FileNamePattern);
        hash.Add(ShouldWriteGuidAsString);
        hash.Add(ShouldEscapeVocabularyKeys);
        hash.Add(CustomCron);
        hash.Add(IsDeltaMode);
        hash.Add(IsOverwriteEnabled);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as DataLakeJobData);
    }

    public bool Equals(DataLakeJobData other)
    {
        return other != null &&
            ContainerName == other.ContainerName &&
            OutputFormat == other.OutputFormat &&
            IsStreamCacheEnabled == other.IsStreamCacheEnabled &&
            StreamCacheConnectionString == other.StreamCacheConnectionString &&
            Schedule == other.Schedule &&
            UseCurrentTimeForExport == other.UseCurrentTimeForExport &&
            FileNamePattern == other.FileNamePattern &&
            ShouldWriteGuidAsString == other.ShouldWriteGuidAsString &&
            ShouldEscapeVocabularyKeys == other.ShouldEscapeVocabularyKeys &&
            CustomCron == other.CustomCron &&
            IsDeltaMode == other.IsDeltaMode &&
            IsOverwriteEnabled == other.IsOverwriteEnabled;
    }

    protected object GetConfigurationValue(string key)
    {
        if (Configurations.TryGetValue(key, out var value))
        {
            return value;
        }
        return null;
    }
}
