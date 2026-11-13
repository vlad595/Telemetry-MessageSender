using System;
using System.Collections.Concurrent;

namespace Telemetry_MessageSender.Services
{
    public class CooldownService
    {
        private readonly ConcurrentDictionary<Guid, DateTimeOffset> _cooldowns = new();
        private readonly TimeSpan _cooldown = TimeSpan.FromMinutes(20);
        public bool CanSend(Guid agentId)
        {
            if (!_cooldowns.TryGetValue(agentId, out var lastSent))
            {
                return true;
            }
            return DateTimeOffset.UtcNow - lastSent >= _cooldown;
        }
        public void MarkSent(Guid agentId)
        {
            _cooldowns[agentId] = DateTimeOffset.UtcNow;
        }
    }
}