using System;
using System.Collections.Generic;

namespace CluedIn.Connector.FileStorage.Common;

internal abstract class StorageConfigurationBase : CrawlJobDataWrapper, IStorageConfiguration
{
    protected StorageConfigurationBase(IDictionary<string, object> configurations, string containerName = null) : base(configurations)
    {
        ContainerName = containerName;
    }

    public string OutputFormat => GetConfigurationValue(StorageConfigurationConstants.OutputFormat) as string ?? StorageConfigurationConstants.OutputFormats.Json;
    public virtual bool IsStreamCacheEnabled => GetConfigurationValue(StorageConfigurationConstants.IsStreamCacheEnabled) as bool? ?? false;
    public string StreamCacheConnectionString => GetConfigurationValue(StorageConfigurationConstants.StreamCacheConnectionString) as string;
    public string Schedule => GetConfigurationValue(StorageConfigurationConstants.Schedule) as string;
    public string ContainerName { get; }
    public bool UseCurrentTimeForExport => GetConfigurationValue(StorageConfigurationConstants.UseCurrentTimeForExport) as bool? ?? false;
    public string FileNamePattern => GetConfigurationValue(StorageConfigurationConstants.FileNamePattern) as string;
    public virtual bool ShouldWriteGuidAsString => GetConfigurationValue(StorageConfigurationConstants.ShouldWriteGuidAsString) as bool? ?? false;
    public virtual bool ShouldEscapeVocabularyKeys => GetConfigurationValue(StorageConfigurationConstants.ShouldEscapeVocabularyKeys) as bool? ?? false;
    public string CustomCron => GetConfigurationValue(StorageConfigurationConstants.CustomCron) as string;
    public virtual bool IsDeltaMode => GetConfigurationValue(StorageConfigurationConstants.IsDeltaMode) as bool? ?? false;

    public virtual bool IsSoftDelete => GetConfigurationValue(StorageConfigurationConstants.IsSoftDelete) as bool? ?? true;
    public virtual bool IsOverwriteEnabled => GetConfigurationValue(StorageConfigurationConstants.IsOverwriteEnabled) as bool? ?? true;
    public virtual bool IsArrayColumnsEnabled => GetConfigurationValue(StorageConfigurationConstants.IsArrayColumnsEnabled) as bool? ?? false;

    public abstract string RootDirectoryPath { get; }

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
        hash.Add(IsArrayColumnsEnabled);
    }

    public override bool Equals(object obj)
    {
        return Equals(obj as StorageConfigurationBase);
    }

    public bool Equals(StorageConfigurationBase other)
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
            IsOverwriteEnabled == other.IsOverwriteEnabled &&
            IsArrayColumnsEnabled == other.IsArrayColumnsEnabled;
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
