using System.Globalization;
using System.Net.Http.Json;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Telemetry_MessageSender.Services;

public sealed record TelegramRecipient(Guid UserId, string UserName, string ChatId);

public sealed record TelegramRules(Guid UserId, string? TelegramChatId, IReadOnlyList<string> Rules);

public sealed class TelegramNotificationService : BackgroundService
{
    private readonly TelegramBotClient _bot;
    private readonly HttpClient _httpClient;
    private readonly ILogger<TelegramNotificationService> _logger;
    private readonly string _authApiUrl;

    public TelegramNotificationService(
        HttpClient httpClient,
        IConfiguration configuration,
        ILogger<TelegramNotificationService> logger)
    {
        string botToken = configuration["Telegram:BotToken"]
            ?? throw new InvalidOperationException("Telegram:BotToken is not configured");
        _authApiUrl = configuration["AuthApiUrl"]?.TrimEnd('/')
            ?? throw new InvalidOperationException("AuthApiUrl is not configured");
        _bot = new TelegramBotClient(botToken);
        _httpClient = httpClient;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int offset = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                Update[] updates = await _bot.GetUpdates(
                    offset: offset,
                    timeout: 30,
                    allowedUpdates: [UpdateType.Message],
                    cancellationToken: stoppingToken);

                foreach (Update update in updates)
                {
                    offset = update.Id + 1;

                    try
                    {
                        await HandleUpdateAsync(update, stoppingToken);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Could not process Telegram update {UpdateId}", update.Id);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Could not receive Telegram updates");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    public Task SendNotificationAsync(long chatId, string text, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(text);
        return _bot.SendMessage(chatId, text, cancellationToken: cancellationToken);
    }

    public Task SendNotificationAsync(string chatId, string text, CancellationToken cancellationToken = default)
    {
        if (!long.TryParse(chatId, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsedChatId))
        {
            throw new ArgumentException("Telegram chat ID must be a number", nameof(chatId));
        }

        return SendNotificationAsync(parsedChatId, text, cancellationToken);
    }

    public async Task<IReadOnlyList<TelegramRecipient>> GetRecipientsAsync(Guid agentId, CancellationToken cancellationToken = default)
    {
        string url = $"{_authApiUrl}/api/user/telegram/agent={agentId:D}";
        using HttpResponseMessage response = await _httpClient.GetAsync(url, cancellationToken);
        response.EnsureSuccessStatusCode();
        InstanceTelegramRecipientsResponse users = await response.Content.ReadFromJsonAsync<InstanceTelegramRecipientsResponse>(cancellationToken)
            ?? throw new InvalidOperationException($"Auth API returned no Telegram recipients for agent {agentId}");
        return users.Users
            .Where(user => !string.IsNullOrWhiteSpace(user.Value.UserChatId))
            .Select(user => new TelegramRecipient(user.Key, user.Value.UserName, user.Value.UserChatId))
            .ToList();
    }

    public async Task<TelegramRules?> GetRulesAsync(Guid userId, CancellationToken cancellationToken = default)
    {
        string url = $"{_authApiUrl}/api/user/telegram/user={userId:D}";
        using HttpResponseMessage response = await _httpClient.GetAsync(url, cancellationToken);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            return null;

        response.EnsureSuccessStatusCode();
        UserTelegramRulesResponse rules = await response.Content.ReadFromJsonAsync<UserTelegramRulesResponse>(cancellationToken)
            ?? throw new InvalidOperationException($"Auth API returned no rules for user {userId}");
        if (rules.UserId != userId)
            throw new InvalidOperationException($"Auth API returned rules for the wrong user {userId}");

        return new TelegramRules(rules.UserId, rules.TelegramChatId, rules.Rules ?? []);
    }

    private sealed record InstanceTelegramRecipientsResponse(Guid InstanceId, string InstanceName,
        string? InstanceWorkspace, Dictionary<Guid, TelegramRecipientResponse> Users);

    private sealed record TelegramRecipientResponse(string UserName, string UserChatId);

    private sealed record UserTelegramRulesResponse(Guid UserId, string? TelegramChatId, List<string>? Rules);

    private sealed record TelegramConnectionResponse(Guid UserId);

    private async Task HandleUpdateAsync(Update update, CancellationToken cancellationToken)
    {
        Message? message = update.Message;
        if (message?.Chat.Type != ChatType.Private || message.Text is null)
        {
            return;
        }

        string[] command = message.Text.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
        if (command.Length == 0 || command[0] != "/start")
        {
            return;
        }

        if (command.Length != 2 || string.IsNullOrWhiteSpace(command[1]))
        {
            await SendNotificationAsync(message.Chat.Id, "Відкрийте посилання для підключення Telegram у вашому профілі Telemetry.", cancellationToken);
            return;
        }

        string code = command[1].Trim();
        if (code.Length > 64 || code.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not ('-' or '_')))
        {
            await SendNotificationAsync(message.Chat.Id, "Некоректний код підключення. Створіть нове посилання в Telemetry.", cancellationToken);
            return;
        }

        string url = $"{_authApiUrl}/api/user/telegram/create-connection?code={Uri.EscapeDataString(code)}&chatId={message.Chat.Id.ToString(CultureInfo.InvariantCulture)}";
        using var request = new HttpRequestMessage(HttpMethod.Patch, url);
        using HttpResponseMessage response = await _httpClient.SendAsync(request, cancellationToken);

        if (response.IsSuccessStatusCode)
        {
            TelegramConnectionResponse? connectedUser = await response.Content.ReadFromJsonAsync<TelegramConnectionResponse>(cancellationToken);
            if (connectedUser is null || connectedUser.UserId == Guid.Empty)
            {
                throw new InvalidOperationException("Auth API returned an invalid Telegram connection response");
            }

            await SendNotificationAsync(message.Chat.Id, "Telegram підключено до акаунта Telemetry.", cancellationToken);
        }
        else if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            await SendNotificationAsync(message.Chat.Id, "Код підключення недійсний або прострочений. Створіть нове посилання в Telemetry.", cancellationToken);
        }
        else
        {
            _logger.LogWarning("Auth API rejected Telegram link with status {StatusCode}", (int)response.StatusCode);
            await SendNotificationAsync(message.Chat.Id, "Не вдалося підключити Telegram. Спробуйте пізніше.", cancellationToken);
        }
    }
}
