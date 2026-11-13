using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Telemetry_MessageSender.Services;

namespace Telemetry_MessageSender;

public class Worker(ILogger<Worker> logger, IConfiguration config, TelegramNotificationService notifications, CooldownService cooldownService) : BackgroundService
{
    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            UserName = config["RabbitMQ:UserName"]
                ?? throw new InvalidOperationException("RabbitMq username is null"),

            Password = config["RabbitMQ:Password"]
                ?? throw new InvalidOperationException("RabbitMq password is null"),

            HostName = config["RabbitMQ:Host"]
                ?? throw new InvalidOperationException("RabbitMq hostname is null")
        };

        await using var connection =
            await factory.CreateConnectionAsync(stoppingToken);

        await using var channel =
            await connection.CreateChannelAsync(
                cancellationToken: stoppingToken);

        await channel.BasicQosAsync(0, 10, false,
            cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: "telegram_notification",
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken
        );

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, args) =>
        {
            try
            {
                JsonDocument document;
                try
                {
                    document = JsonDocument.Parse(args.Body);
                }
                catch (JsonException ex)
                {
                    logger.LogWarning(ex, "Invalid Telegram notification with delivery tag {DeliveryTag}", args.DeliveryTag);
                    await channel.BasicNackAsync(args.DeliveryTag, false, false, stoppingToken);
                    return;
                }

                using (document)
                {
                    JsonElement metric = document.RootElement;
                    if (metric.ValueKind != JsonValueKind.Object
                        || !metric.TryGetProperty("instanceId", out JsonElement id)
                        || !Guid.TryParse(id.ToString(), out Guid agentId)
                        || agentId == Guid.Empty)
                    {
                        logger.LogWarning("Skipping invalid metric notification with delivery tag {DeliveryTag}", args.DeliveryTag);
                        await channel.BasicNackAsync(args.DeliveryTag, false, false, stoppingToken);
                        return;
                    }

                    InstanceTelegramRecipients instance = await notifications.GetRecipientsAsync(agentId, stoppingToken);
                    if (instance.Recipients.Count == 0)
                        logger.LogInformation("Agent {AgentId} has no Telegram recipients", agentId);

                    bool deliveryFailed = false;
                    foreach (TelegramRecipient recipient in instance.Recipients)
                    {
                        try
                        {
                            TelegramRules? rules = await notifications.GetRulesAsync(recipient.UserId, stoppingToken);
                            if (rules is null || string.IsNullOrWhiteSpace(rules.TelegramChatId))
                                continue;

                            IReadOnlyList<MatchedAlertRule> matches = AlertRuleEvaluator.GetMatches(metric, rules.Rules);
                            if (matches.Count == 0)
                                continue;
                            
                            if (!cooldownService.CanSend(agentId)) continue;

                            string text = FormatNotification(metric, instance, matches);
                            await notifications.SendNotificationAsync(rules.TelegramChatId, text, stoppingToken, html: true);

                            cooldownService.MarkSent(agentId);

                            logger.LogInformation("Telegram notification sent to user {UserId} for agent {AgentId}", recipient.UserId, agentId);
                        }
                        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Could not send Telegram notification to user {UserId} for agent {AgentId}", recipient.UserId, agentId);
                            deliveryFailed = true;
                        }
                    }

                    if (deliveryFailed)
                        throw new InvalidOperationException($"One or more Telegram notifications failed for agent {agentId}");
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                await channel.BasicNackAsync(args.DeliveryTag, false, true);
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing message");

                await channel.BasicNackAsync(args.DeliveryTag, false, true, stoppingToken);

                return;
            }

            await channel.BasicAckAsync(args.DeliveryTag, false, stoppingToken);
        };

        await channel.BasicConsumeAsync(queue: "telegram_notification", autoAck: false, consumer: consumer, cancellationToken: stoppingToken);

        logger.LogInformation("RabbitMQ consumer started");

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("RabbitMQ consumer stopped");
        }
    }

    private static string FormatNotification(JsonElement metric, InstanceTelegramRecipients instance,
        IReadOnlyList<MatchedAlertRule> matches)
    {
        var text = new StringBuilder("🚨 <b>Telemetry · сповіщення</b>\n\n");
        text.Append("🖥️ <b>Інстанс:</b> ")
            .Append(WebUtility.HtmlEncode(DisplayText(instance.InstanceName, "Невідомий інстанс"))).Append('\n');

        if (!string.IsNullOrWhiteSpace(instance.WorkspaceName))
            text.Append("🏢 <b>Воркспейс:</b> ")
                .Append(WebUtility.HtmlEncode(DisplayText(instance.WorkspaceName, ""))).Append('\n');

        if (TryGetProperty(metric, "timestamp", out JsonElement timestamp)
            && DateTimeOffset.TryParse(timestamp.ToString(), CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal, out DateTimeOffset time))
            text.Append("🕒 <b>Час:</b> ")
                .Append(time.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss 'UTC'", CultureInfo.InvariantCulture))
                .Append('\n');

        text.Append("\n📌 <b>Спрацювали правила:</b>\n");
        foreach (MatchedAlertRule match in matches)
        {
            string line = FormatMatch(match);
            if (text.Length + line.Length + 40 > 4000)
            {
                text.Append("… та інші правила\n");
                break;
            }
            text.Append(line);
        }
        return text.ToString();
    }

    private static string FormatMatch(MatchedAlertRule match)
    {
        var line = new StringBuilder();
        (string icon, string label) = FieldLabel(match.Field);
        line.Append(icon).Append(" <b>").Append(WebUtility.HtmlEncode(label)).Append(":</b> ")
            .Append(WebUtility.HtmlEncode(FormatValue(match.Field, match.Value)));

        if (match.ComparedField is not null)
        {
            (_, string comparedLabel) = FieldLabel(match.ComparedField);
            line.Append(" · <b>").Append(WebUtility.HtmlEncode(comparedLabel)).Append(":</b> ")
                .Append(WebUtility.HtmlEncode(FormatValue(match.ComparedField, match.ComparedValue)));
        }

        line.Append("\n<code>").Append(WebUtility.HtmlEncode(DisplayText(match.Rule, "", 180)))
            .Append("</code>\n");
        return line.ToString();
    }

    private static (string Icon, string Label) FieldLabel(string field) => field.ToLowerInvariant() switch
    {
        "cpuload" => ("🔥", "CPU"),
        "ramload" => ("🧠", "RAM"),
        "totalram" => ("💾", "Загальна RAM"),
        "freediskspace" => ("💽", "Вільне місце"),
        "wifirx" => ("📥", "Вхідний трафік"),
        "wifitx" => ("📤", "Вихідний трафік"),
        "opensockets" => ("🔌", "Відкриті сокети"),
        "readingspeed" => ("📖", "Читання диска"),
        "writingspeed" => ("✍️", "Запис диска"),
        _ => ("📊", field)
    };

    private static string FormatValue(string field, JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.Number || !value.TryGetDouble(out double number))
            return DisplayText(value.ToString(), "—", 120);

        string name = field.ToLowerInvariant();
        if (name is "cpuload" or "ramload")
            return $"{(number * 100).ToString("0.#", CultureInfo.GetCultureInfo("uk-UA"))} %";
        if (name is "totalram" or "freediskspace")
            return $"{number.ToString("N0", CultureInfo.GetCultureInfo("uk-UA"))} MB";
        if (name is "wifirx" or "wifitx" or "readingspeed" or "writingspeed")
            return FormatRate(number);
        if (name == "opensockets")
            return number.ToString("N0", CultureInfo.GetCultureInfo("uk-UA"));
        return number.ToString("0.##", CultureInfo.GetCultureInfo("uk-UA"));
    }

    private static string FormatRate(double bytesPerSecond)
    {
        string unit = "B/s";
        if (Math.Abs(bytesPerSecond) >= 1024 * 1024)
        {
            bytesPerSecond /= 1024 * 1024;
            unit = "MB/s";
        }
        else if (Math.Abs(bytesPerSecond) >= 1024)
        {
            bytesPerSecond /= 1024;
            unit = "KB/s";
        }
        return $"{bytesPerSecond.ToString("0.#", CultureInfo.GetCultureInfo("uk-UA"))} {unit}";
    }

    private static string DisplayText(string? value, string fallback, int maxLength = 120)
    {
        string text = string.Join(' ', (value ?? "").Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        if (text.Length == 0) return fallback;
        if (text.Length <= maxLength) return text;
        int length = char.IsHighSurrogate(text[maxLength - 1]) ? maxLength - 1 : maxLength;
        return text[..length] + "…";
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
