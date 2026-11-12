using System.Text.Json;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Telemetry_MessageSender.Services;

namespace Telemetry_MessageSender;

public class Worker(ILogger<Worker> logger, IConfiguration config, TelegramNotificationService notifications) : BackgroundService
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

                    IReadOnlyList<TelegramRecipient> recipients = await notifications.GetRecipientsAsync(agentId, stoppingToken);
                    if (recipients.Count == 0)
                        logger.LogInformation("Agent {AgentId} has no Telegram recipients", agentId);

                    bool deliveryFailed = false;
                    string text = FormatNotification(metric, agentId);
                    foreach (TelegramRecipient recipient in recipients)
                    {
                        try
                        {
                            TelegramRules? rules = await notifications.GetRulesAsync(recipient.UserId, stoppingToken);
                            if (rules is null || string.IsNullOrWhiteSpace(rules.TelegramChatId)
                                || !AlertRuleEvaluator.MatchesAny(metric, rules.Rules))
                                continue;

                            await notifications.SendNotificationAsync(rules.TelegramChatId, text, stoppingToken);
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

    private static string FormatNotification(JsonElement metric, Guid agentId)
    {
        var text = new StringBuilder($"⚠️ Telemetry: {agentId:D}");
        foreach (JsonProperty property in metric.EnumerateObject())
        {
            if (string.Equals(property.Name, "instanceId", StringComparison.OrdinalIgnoreCase))
                continue;
            text.Append('\n').Append(property.Name).Append(": ").Append(property.Value.ToString());
        }
        return text.Length <= 4096 ? text.ToString() : text.ToString(0, 4093) + "...";
    }
}
