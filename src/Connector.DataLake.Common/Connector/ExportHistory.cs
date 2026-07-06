using System;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal record ExportHistory(
    Guid StreamId,
    DateTimeOffset DataTime,
    string TriggerSource,
    string CronSchedule,
    string FilePath,
    string FileFormat,
    DateTimeOffset StartTime,
    DateTimeOffset? EndTime,
    long? TotalRows,
    string Status,
    string ExporterHostName);
