using EventBus.Base.Abstraction;
using EventBus.Base.Events;

namespace EventBus.Base.SubManagers;

public class InMemoryBusSubscriptionManager : IEventBusSubscriptionManager
{
    private readonly Dictionary<string, List<SubscriptionInfo>> _handlers;
    private readonly List<Type> _eventTypes;

    public Func<string, string> eventNameGetter;

    public bool IsEmpty => !_handlers.Keys.Any();
    public void Clear() => _handlers.Clear();
    
    public event EventHandler<string> OnEventRemoved;

    public InMemoryBusSubscriptionManager(Func<string, string> eventNameGetter)
    {
        _handlers = new Dictionary<string, List<SubscriptionInfo>>();
        _eventTypes = new List<Type>();
        this.eventNameGetter = eventNameGetter;
    }

    public void AddSubscription<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>
    {
        string eventName = GetEventKey<T>();
        AddSubscription(typeof(TH), eventName);
        if (!_eventTypes.Contains(typeof(TH)))
        {
            _eventTypes.Add(typeof(TH));
        }
    }

    public void RemoveSubscription<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>
    {
        string eventName = GetEventKey<T>();
        SubscriptionInfo? subscriptionInfo = FindSubscriptionsForRemove<T, TH>();
        RemoveHandler(eventName,subscriptionInfo);
    }

    public bool HasSubscriptionsForEvent<T>() where T : IntegrationEvent
    {
        string eventName = GetEventKey<T>();
        return HasSubscriptionsForEvent(eventName);
    }

    public Type? GetEventTypeByName(string eventName)
    {
        return _eventTypes.SingleOrDefault(x => x.Name == eventName);
    }

    public IEnumerable<SubscriptionInfo> GetHandlersForEvent<T>() where T : IntegrationEvent
    {
        string eventName = GetEventKey<T>();
        return GetHandlersForEvent(eventName);
    }

    public IEnumerable<SubscriptionInfo> GetHandlersForEvent(string eventName)
    {
        return _handlers[eventName];
    }

    public string GetEventKey<T>()
    {
        string eventName = typeof(T).Name;
        return eventNameGetter(eventName);
    }

    public bool HasSubscriptionsForEvent(string eventName) => _handlers.ContainsKey(eventName);

    private void AddSubscription(Type handlerType, string eventName)
    {
        if (!HasSubscriptionsForEvent(eventName))
        {
            _handlers.Add(eventName, new List<SubscriptionInfo>());
        }

        if (_handlers[eventName].Any(x => x.HandlerType == handlerType))
        {
            throw new ArgumentException($"Handler type {handlerType.Name} already registered");
        }
        
        _handlers[eventName].Add(SubscriptionInfo.Typed(handlerType));
    }

    private void RemoveHandler(string eventName, SubscriptionInfo? subsToRemove)
    {
        if (subsToRemove != null)
        {
            _handlers[eventName].Remove(subsToRemove);
            if (!_handlers[eventName].Any())
            {
                _handlers.Remove(eventName);
                Type? eventType = _eventTypes.SingleOrDefault(x => x.Name == eventName);
                if (eventType != null)
                {
                    _eventTypes.Remove(eventType);
                }
                RaiseOnRemoveEvent(eventName);
            }
        }
        
    }

    private void RaiseOnRemoveEvent(string eventName)
    {
        var handler = OnEventRemoved;
        handler?.Invoke(this, eventName);
    }

    private SubscriptionInfo? FindSubscriptionsForRemove<T, TH>()
        where T : IntegrationEvent where TH : IIntegrationEventHandler<T>
    {
        string eventName = GetEventKey<T>();
        return FindSubscriptionsForRemove(eventName, typeof(TH));
    }

    private SubscriptionInfo? FindSubscriptionsForRemove(string eventName, Type handlerType)
    {
        if (!HasSubscriptionsForEvent(eventName))
        {
            return null;
        }
        return _handlers[eventName].SingleOrDefault(x => x.HandlerType == handlerType);
    }
}