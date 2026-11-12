using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Telemetry_MessageSender.Services;

namespace Telemetry_MessageSender;

public class Worker(ILogger<Worker> logger, IConfiguration config, TelegramNotificationService notifications) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private sealed record NotificationMessage(Guid AgentId, string Message);

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
                NotificationMessage? message;
                try
                {
                    message = JsonSerializer.Deserialize<NotificationMessage>(args.Body.Span, JsonOptions);
                }
                catch (JsonException ex)
                {
                    logger.LogWarning(ex, "Invalid Telegram notification with delivery tag {DeliveryTag}", args.DeliveryTag);
                    await channel.BasicNackAsync(args.DeliveryTag, false, false, stoppingToken);
                    return;
                }

                if (message is null || message.AgentId == Guid.Empty || string.IsNullOrWhiteSpace(message.Message))
                {
                    logger.LogWarning("Skipping invalid Telegram notification with delivery tag {DeliveryTag}", args.DeliveryTag);
                    await channel.BasicNackAsync(args.DeliveryTag, false, false, stoppingToken);
                    return;
                }

                IReadOnlyList<TelegramRecipient> recipients = await notifications.GetRecipientsAsync(message.AgentId, stoppingToken);
                if (recipients.Count == 0)
                {
                    logger.LogInformation("Agent {AgentId} has no Telegram recipients", message.AgentId);
                }

                bool deliveryFailed = false;
                foreach (TelegramRecipient recipient in recipients)
                {
                    try
                    {
                        await notifications.SendNotificationAsync(recipient.ChatId, message.Message, stoppingToken);
                        logger.LogInformation("Telegram notification sent to user {UserId} for agent {AgentId}", recipient.UserId, message.AgentId);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Could not send Telegram notification to user {UserId} for agent {AgentId}", recipient.UserId, message.AgentId);
                        deliveryFailed = true;
                    }
                }

                if (deliveryFailed)
                {
                    throw new InvalidOperationException($"One or more Telegram notifications failed for agent {message.AgentId}");
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
}
