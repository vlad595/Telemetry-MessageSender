using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Telemetry_MessageSender.Services;

public sealed record MatchedAlertRule(
    string Rule,
    string Field,
    JsonElement Value,
    string? ComparedField = null,
    JsonElement ComparedValue = default);

public static class AlertRuleEvaluator
{
    private static readonly Regex Condition = new(
        @"^\s*(?<field>[A-Za-z][A-Za-z0-9_]*)\s*(?<operator>>=|<=|==|!=|>|<|=)\s*(?<value>.+?)\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static IReadOnlyList<MatchedAlertRule> GetMatches(JsonElement metric, IEnumerable<string> rules)
    {
        var matches = new List<MatchedAlertRule>();
        foreach (string rule in rules)
        {
            if (TryEvaluate(metric, rule, out bool matched, out MatchedAlertRule? detail) && matched && detail is not null)
                matches.Add(detail);
        }
        return matches;
    }

    public static bool MatchesAny(JsonElement metric, IEnumerable<string> rules)
    {
        foreach (string rule in rules)
        {
            if (TryMatch(metric, rule, out bool matches) && matches)
                return true;
        }
        return false;
    }

    public static bool TryMatch(JsonElement metric, string? rule, out bool matches) =>
        TryEvaluate(metric, rule, out matches, out _);

    private static bool TryEvaluate(JsonElement metric, string? rule, out bool matches, out MatchedAlertRule? detail)
    {
        matches = false;
        detail = null;
        if (metric.ValueKind != JsonValueKind.Object || string.IsNullOrWhiteSpace(rule))
            return false;

        Match condition = Condition.Match(rule);
        string field = condition.Groups["field"].Value;
        if (!condition.Success || !TryGetProperty(metric, field, out JsonElement left))
            return false;

        string rightText = condition.Groups["value"].Value.Trim();
        string comparison = condition.Groups["operator"].Value;
        bool rightIsField = TryGetProperty(metric, rightText, out JsonElement rightField);

        if (left.ValueKind == JsonValueKind.Number && left.TryGetDouble(out double leftNumber))
        {
            leftNumber = NormalizeNumber(field, leftNumber);
            double rightNumber;
            if (rightIsField)
            {
                if (rightField.ValueKind != JsonValueKind.Number || !rightField.TryGetDouble(out rightNumber))
                    return false;
                rightNumber = NormalizeNumber(rightText, rightNumber);
            }
            else if (!double.TryParse(rightText, NumberStyles.Float, CultureInfo.InvariantCulture, out rightNumber))
            {
                return false;
            }

            matches = comparison switch
            {
                ">" => leftNumber > rightNumber,
                ">=" => leftNumber >= rightNumber,
                "<" => leftNumber < rightNumber,
                "<=" => leftNumber <= rightNumber,
                "=" or "==" => leftNumber == rightNumber,
                "!=" => leftNumber != rightNumber,
                _ => false
            };
        }
        else
        {
            if (left.ValueKind is not (JsonValueKind.String or JsonValueKind.True or JsonValueKind.False)
                || comparison is not ("=" or "==" or "!="))
                return false;

            string expected = rightIsField ? rightField.ToString() : rightText.Trim('"', '\'');
            bool equal = string.Equals(left.ToString(), expected, StringComparison.OrdinalIgnoreCase);
            matches = comparison == "!=" ? !equal : equal;
        }

        if (matches)
            detail = new MatchedAlertRule(rule.Trim(), field, left, rightIsField ? rightText : null, rightField);
        return true;
    }

    private static double NormalizeNumber(string field, double value) =>
        field.Equals("CpuLoad", StringComparison.OrdinalIgnoreCase)
        || field.Equals("RamLoad", StringComparison.OrdinalIgnoreCase)
            ? value * 100
            : value;

    private static bool TryGetProperty(JsonElement metric, string name, out JsonElement value)
    {
        foreach (JsonProperty property in metric.EnumerateObject())
        {
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }
        value = default;
        return false;
    }
}
