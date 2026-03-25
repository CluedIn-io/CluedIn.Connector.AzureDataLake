using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using CluedIn.Core;

namespace CluedIn.Connector.DataLake.Common.Connector;

internal class PatternHelper
{
    public static Task<string> ReplaceNameUsingPatternAsync(ExecutionContext context, string pattern, Guid streamId, string containerName, DateTimeOffset dataTime, string outputFormat)
    {
        var timeRegexPattern = @"\{(DataTime)(\:[a-zA-Z0-9\-\._]+)?\}";
        var streamIdRegexPattern = @"\{(StreamId)(\:[a-zA-Z0-9\-\._]+)?\}";
        var containerNameRegexPattern = @"\{(ContainerName)(\:[a-zA-Z0-9\-\._]+)?\}";
        var outputFormatRegexPattern = @"\{(OutputFormat)(\:[a-zA-Z0-9\-\._]+)?\}";

        var timeReplaced = Replace(timeRegexPattern, pattern, (match, format) => dataTime.ToString(format ?? "o"));
        var streamIdReplaced = Replace(streamIdRegexPattern, timeReplaced, (match, format) => streamId.ToString(format ?? "D"));
        var containerNameReplaced = Replace(containerNameRegexPattern, streamIdReplaced, (match, format) => containerName);
        var outputFormatReplaced = Replace(outputFormatRegexPattern, containerNameReplaced, (match, format) =>
        {
            return format?.ToLowerInvariant() switch
            {
                "toupper" => outputFormat.ToUpperInvariant(),
                "toupperinvariant" => outputFormat.ToUpperInvariant(),
                "tolower" => outputFormat.ToLowerInvariant(),
                "tolowerinvariant" => outputFormat.ToLowerInvariant(),
                null => outputFormat,
                _ => throw new NotSupportedException($"Format '{format}' is not supported"),
            };
        });

        return Task.FromResult(outputFormatReplaced);
    }

    private static string Replace(string pattern, string input, Func<Match, string, string> formatter)
    {
        var regex = new Regex(pattern);
        var matches = regex.Matches(input);
        var result = input;
        foreach (var match in matches.Reverse())
        {
            if (match.Groups.Count != 3 || !match.Groups[1].Success)
            {
                continue;
            }
            var format = match.Groups[2].Success
                ? match.Groups[2].Captures.Single().Value[1..]
                : null;
            var formatted = formatter(match, format);
            result = $"{result[0..match.Index]}{formatted}{result[(match.Index + match.Length)..]}";
        }

        return result;
    }
}
