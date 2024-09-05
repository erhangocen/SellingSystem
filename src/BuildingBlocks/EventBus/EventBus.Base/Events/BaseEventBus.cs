using System.Net.Http.Json;
using EventBus.Base.Abstraction;
using EventBus.Base.SubManagers;
using Newtonsoft.Json;
using Microsoft.Extensions.DependencyInjection;

namespace EventBus.Base.Events;

public abstract class BaseEventBus : IEventBus
{
    public readonly IServiceProvider ServiceProvider;
    public readonly IEventBusSubscriptionManager SubscriptionManager;

    public EventBusConfig? EventBusConfig { get; set; }

    public BaseEventBus(IServiceProvider serviceProvider, EventBusConfig eventBusConfig)
    {
        ServiceProvider = serviceProvider;
        SubscriptionManager = new InMemoryBusSubscriptionManager(ProcessEventName);
        EventBusConfig = eventBusConfig;
    }

    public virtual string ProcessEventName(string eventName)
    {
        if (EventBusConfig != null)
        {
            if (EventBusConfig.DeleteEventPrefix)
            {
                eventName = eventName.TrimStart(EventBusConfig.EventNamePrefix.ToArray());
            }

            if (EventBusConfig.DeleteEventSuffix)
            {
                eventName = eventName.TrimEnd(EventBusConfig.EventNameSuffix.ToArray());
            }
        }
        return eventName;
    }

    public virtual string GetSubName(string eventName)
    {
        return $"{EventBusConfig?.SubscriberClientAppName}.{ProcessEventName(eventName)}";
    }

    public virtual void Dispose()
    {
        EventBusConfig = null;
        SubscriptionManager.Clear();
    }

    public async Task<bool>? ProcessEvent(string eventName, string message)
    {
        eventName = ProcessEventName(eventName);
        bool processed = false;
        if (SubscriptionManager.HasSubscriptionsForEvent(eventName))
        {
            IEnumerable<SubscriptionInfo> subscriptions = SubscriptionManager.GetHandlersForEvent(eventName);
            using (var scope = ServiceProvider.CreateScope())
            {
                foreach (SubscriptionInfo subscription in subscriptions)
                {
                    var handler = ServiceProvider.GetService(subscription.HandlerType);
                    if (handler == null) continue;
                    
                    Type? eventType = SubscriptionManager.GetEventTypeByName($"{EventBusConfig?.EventNamePrefix}{eventName}{EventBusConfig?.EventNameSuffix}");
                    
                    var integrationEvent = JsonConvert.DeserializeObject(message, eventType);
                    Type? concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);

                    await (Task)concreteType.GetMethod("Handle").Invoke(handler, new object[] { integrationEvent });
                }
            }
            processed = true;
        }

        return processed;
    }

    public abstract void Publish(IntegrationEvent @event);

    public abstract void Subscribe<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>;

    public abstract void UnSubscribe<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>;
}