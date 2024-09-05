using EventBus.AzureServiceBus;
using EventBus.Base;
using EventBus.Base.Abstraction;
using EventBus.RabbitMQ;

namespace EventBus.Factory;

public class EventBusFactory
{
    public static IEventBus Create(EventBusConfig config, IServiceProvider serviceProvider)
    {
        return config.EventBusType switch
        {
            EventBusType.AzureServiceBus => new EventBusServiceBus(serviceProvider, config),
            _ => new EventBusRabbitMQ(serviceProvider, config)
        };
    }
}