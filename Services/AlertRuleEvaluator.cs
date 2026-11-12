using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Telemetry_MessageSender.Services;

public static class AlertRuleEvaluator
{
    private static readonly Regex Condition = new(
        @"^\s*(?<field>[A-Za-z][A-Za-z0-9_]*)\s*(?<operator>>=|<=|==|!=|>|<|=)\s*(?<value>.+?)\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static bool MatchesAny(JsonElement metric, IEnumerable<string> rules)
    {
        foreach (string rule in rules)
        {
            if (TryMatch(metric, rule, out bool matches) && matches)
                return true;
        }
        return false;
    }

    public static bool TryMatch(JsonElement metric, string? rule, out bool matches)
    {
        matches = false;
        if (metric.ValueKind != JsonValueKind.Object || string.IsNullOrWhiteSpace(rule))
            return false;

        Match condition = Condition.Match(rule);
        if (!condition.Success || !TryGetProperty(metric, condition.Groups["field"].Value, out JsonElement left))
            return false;

        string rightText = condition.Groups["value"].Value.Trim();
        string comparison = condition.Groups["operator"].Value;
        bool rightIsField = TryGetProperty(metric, rightText, out JsonElement rightField);

        if (left.ValueKind == JsonValueKind.Number && left.TryGetDouble(out double leftNumber))
        {
            double rightNumber;
            if (rightIsField)
            {
                if (rightField.ValueKind != JsonValueKind.Number || !rightField.TryGetDouble(out rightNumber))
                    return false;
            }
            else if (!double.TryParse(rightText, NumberStyles.Float, CultureInfo.InvariantCulture, out rightNumber))
            {
                return false;
            }

            matches = comparison switch
            {
                ">" => leftNumber * 100 > rightNumber,
                ">=" => leftNumber * 100 >= rightNumber,
                "<" => leftNumber * 100 < rightNumber,
                "<=" => leftNumber * 100 <= rightNumber,
                "=" or "==" => leftNumber * 100 == rightNumber,
                "!=" => leftNumber * 100 != rightNumber,
                _ => false
            };
            return true;
        }

        if (left.ValueKind is not (JsonValueKind.String or JsonValueKind.True or JsonValueKind.False)
            || comparison is not ("=" or "==" or "!="))
            return false;

        string leftText = left.ToString();
        string expected = rightIsField ? rightField.ToString() : rightText.Trim('"', '\'');
        bool equal = string.Equals(leftText, expected, StringComparison.OrdinalIgnoreCase);
        matches = comparison == "!=" ? !equal : equal;
        return true;
    }

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
