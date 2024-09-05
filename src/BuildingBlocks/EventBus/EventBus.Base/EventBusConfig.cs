namespace EventBus.Base;

public class EventBusConfig
{
    public int ConnectionRetryCount { get; set; } = 5;
    public string DefaultTopicName { get; set; } = "SellingSystemEventBus";
    public string EventBusConnectionString { get; set; } = string.Empty;
    public string SubscriberClientAppName { get; set; } = string.Empty;
    public string EventNamePrefix { get; set; } = string.Empty;
    public string EventNameSuffix { get; set; } = "IntegrationEvent";

    public object Connection { get; set; }
    public EventBusType EventBusType { get; set; } = EventBusType.RabbitMQ;


    public bool DeleteEventPrefix => !string.IsNullOrEmpty(EventNamePrefix);
    public bool DeleteEventSuffix => !string.IsNullOrEmpty(EventNameSuffix);
}