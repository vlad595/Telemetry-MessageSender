using Telemetry_MessageSender;
using Telemetry_MessageSender.Services;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();
builder.Services.AddSingleton(new HttpClient { Timeout = TimeSpan.FromSeconds(10) });
builder.Services.AddSingleton<TelegramNotificationService>();
builder.Services.AddSingleton<CooldownService>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<TelegramNotificationService>());

var host = builder.Build();
host.Run();
